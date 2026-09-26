package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.gbeaule.keycloaknats.consumer.InboxProcessor;
import io.nats.client.PullSubscribeOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * Disposable coordinated restore and fenced synchronous promotion; never targets an external DB.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class PostgresRecoveryIT extends IntegrationSupport {
  @TempDir Path backups;

  @BeforeEach
  void start() throws Exception {
    startInfrastructure();
    InboxProcessor.initialize(database);
  }

  @AfterEach
  void stop() throws Exception {
    stopInfrastructure();
  }

  @Test
  void coordinatedColdBackupRestoresPendingCaptureAndPreviouslyCommittedInbox() throws Exception {
    createUser();
    drained();
    final byte[] committed = consumeOne();
    assertEquals(0, messages());

    broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    final String pendingUser = createUser();
    keycloak.stop();
    final byte[] pending = pendingPayload();
    checked(postgres, "pg_dump", "-U", "keycloak", "-d", "keycloak", "-Fc", "-f", "/tmp/knd.dump");
    Path dump = backups.resolve("postgres.dump");
    postgres.copyFileFromContainer("/tmp/knd.dump", dump.toString());
    Path stream = backups.resolve("jetstream.tar");
    try (var archive =
        broker
            .getDockerClient()
            .copyArchiveFromContainerCmd(broker.getContainerId(), "/data/jetstream")
            .exec()) {
      Files.copy(archive, stream);
    }

    // Both stores are replaced with fresh containers from the same quiescent recovery boundary.
    nats.close();
    broker.stop();
    postgres.stop();
    postgres =
        postgresContainer()
            .withDatabaseName("keycloak")
            .withUsername("keycloak")
            .withPassword("integration-password")
            .withNetwork(network)
            .withNetworkAliases("postgres");
    postgres.start();
    postgres.copyFileToContainer(MountableFile.forHostPath(dump), "/tmp/knd.dump");
    checked(
        postgres,
        "pg_restore",
        "-U",
        "keycloak",
        "-d",
        "keycloak",
        "--exit-on-error",
        "/tmp/knd.dump");
    database = dataSource(postgres.getHost(), postgres.getMappedPort(5432));
    assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
    assertArrayEquals(pending, pendingPayload());
    assertUserPresent(pendingUser);
    var processor = new InboxProcessor(database, DURABLE);
    assertFalse(processor.process(committed, processor::recordEffect));

    broker =
        new GenericContainer<>(NATS_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("nats")
            .withExposedPorts(4222)
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("nats.conf"), "/etc/nats/test.conf")
            .withCopyFileToContainer(MountableFile.forHostPath(stream), "/restore.tar")
            .withCommand(
                "sh",
                "-c",
                "mkdir -p /data && tar -xf /restore.tar -C /data"
                    + " && exec nats-server -c /etc/nats/test.conf");
    broker.start();
    connectNats();
    assertEquals(0, messages());
    keycloak = keycloakContainer(false);
    keycloak.start();
    drained();
    assertArrayEquals(pending, consumeOne());
    assertEquals(2, scalar("SELECT count(*) FROM knd_effects"));
    assertEquals(2, scalar("SELECT count(*) FROM knd_inbox"));
    assertEquals(0, messages());
  }

  @Test
  void fencedSynchronousPromotionPreservesAcknowledgedAccountAndPendingEvent() throws Exception {
    checked(
        postgres,
        "sh",
        "-c",
        "printf '\nhost replication keycloak 0.0.0.0/0 scram-sha-256\n'"
            + " >> \"$PGDATA/pg_hba.conf\"");
    execute("SELECT pg_reload_conf()");
    try (var standby =
        new GenericContainer<>(POSTGRES_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("standby")
            .withExposedPorts(5432)
            .withEnv("PGPASSWORD", "integration-password")
            .withCommand(
                "sh",
                "-c",
                "mkdir -p /var/lib/postgresql/standby"
                    + " && chown postgres:postgres /var/lib/postgresql/standby"
                    + " && chmod 700 /var/lib/postgresql/standby"
                    + " && gosu postgres pg_basebackup"
                    + " -d 'host=postgres user=keycloak application_name=knd_standby'"
                    + " -D /var/lib/postgresql/standby -R -X stream"
                    + " && exec gosu postgres postgres -D /var/lib/postgresql/standby"
                    + " -c fsync=on -c synchronous_commit=on")
            .waitingFor(
                Wait.forLogMessage(
                    ".*database system is ready to accept read-only connections.*\\n", 1))
            .withStartupTimeout(Duration.ofMinutes(2))) {
      standby.start();
      execute("ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (knd_standby)'");
      execute("SELECT pg_reload_conf()");
      await()
          .atMost(Duration.ofSeconds(15))
          .until(
              () ->
                  scalar("SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync'") == 1);
      broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
      loginAdmin();
      final String user = createUser();
      final byte[] pending = pendingPayload();

      // Hard stop is the fence. The former primary is never restarted or made writable again.
      postgres
          .getDockerClient()
          .killContainerCmd(postgres.getContainerId())
          .withSignal("KILL")
          .exec();
      keycloak.stop();
      checked(
          standby,
          "gosu",
          "postgres",
          "pg_ctl",
          "-D",
          "/var/lib/postgresql/standby",
          "promote",
          "-w",
          "-t",
          "30");
      database = dataSource(standby.getHost(), standby.getMappedPort(5432));
      execute("ALTER SYSTEM SET synchronous_standby_names = ''");
      execute("SELECT pg_reload_conf()");
      assertEquals(0, scalar("SELECT CASE WHEN pg_is_in_recovery() THEN 1 ELSE 0 END"));
      assertUserPresent(user);
      assertArrayEquals(pending, pendingPayload());
      broker.getDockerClient().startContainerCmd(broker.getContainerId()).exec();
      connectNats();
      keycloak =
          keycloakContainer(false).withEnv("KC_DB_URL", "jdbc:postgresql://standby:5432/keycloak");
      keycloak.start();
      drained();
      assertArrayEquals(pending, consumeOne());
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
      assertEquals(0, messages());
      keycloak.stop();
    }
  }

  private static byte[] consumeOne() throws Exception {
    var subscription = nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
    try {
      var delivered = subscription.fetch(1, Duration.ofSeconds(10));
      assertEquals(1, delivered.size());
      var message = delivered.getFirst();
      var processor = new InboxProcessor(database, DURABLE);
      assertTrue(processor.process(message.getData(), processor::recordEffect));
      message.ackSync(Duration.ofSeconds(2));
      return message.getData();
    } finally {
      subscription.unsubscribe();
    }
  }

  private static byte[] pendingPayload() throws Exception {
    try (var connection = database.getConnection();
        var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT payload FROM kc_nats_outbox")) {
      assertTrue(rows.next());
      byte[] result = rows.getBytes(1);
      assertFalse(rows.next());
      return result;
    }
  }

  private static void assertUserPresent(String id) throws Exception {
    try (var connection = database.getConnection();
        var query = connection.prepareStatement("SELECT count(*) FROM user_entity WHERE id=?")) {
      query.setString(1, id);
      try (var rows = query.executeQuery()) {
        rows.next();
        assertEquals(1, rows.getInt(1));
      }
    }
  }

  private static PGSimpleDataSource dataSource(String host, int port) {
    var result = new PGSimpleDataSource();
    result.setURL("jdbc:postgresql://" + host + ":" + port + "/keycloak");
    result.setUser("keycloak");
    result.setPassword("integration-password");
    result.setConnectTimeout(5);
    result.setSocketTimeout(10);
    return result;
  }

  private static void checked(GenericContainer<?> container, String... command) throws Exception {
    var result = container.execInContainer(command);
    assertEquals(0, result.getExitCode(), result.getStderr());
  }
}
