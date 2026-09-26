package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.gbeaule.keycloaknats.tls.TlsConfig;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Real three-replica JetStream with client/route mTLS and two Keycloak nodes sharing PostgreSQL.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class NatsClusterIT extends IntegrationSupport {
  @TempDir static Path certificatesDirectory;
  private static TestCertificates certificates;
  private static final List<GenericContainer<?>> brokers = new ArrayList<>();
  private static GenericContainer<?> second;

  @BeforeAll
  static void start() throws Exception {
    certificates = new TestCertificates(certificatesDirectory);
    network = Network.newNetwork();
    postgres =
        new PostgreSQLContainer("postgres:17.6-alpine")
            .withDatabaseName("keycloak")
            .withUsername("keycloak")
            .withPassword("integration-password")
            .withNetwork(network)
            .withNetworkAliases("postgres");
    postgres.start();
    database = new PGSimpleDataSource();
    database.setURL(postgres.getJdbcUrl());
    database.setUser(postgres.getUsername());
    database.setPassword(postgres.getPassword());
    for (int i = 1; i <= 3; i++) {
      String config =
          """
          server_name: nats%d
          port: 4222
          client_advertise: nats%d:4222
          jetstream { store_dir: /data/jetstream, sync_interval: always }
          tls {
            cert_file: /certs/server.pem
            key_file: /certs/server.key
            ca_file: /certs/ca.pem
            verify: true
          }
          cluster {
            name: knd-test
            port: 6222
            routes: [nats://nats1:6222, nats://nats2:6222, nats://nats3:6222]
            tls {
              cert_file: /certs/server.pem
              key_file: /certs/server.key
              ca_file: /certs/ca.pem
              verify: true
            }
          }
          """
              .formatted(i, i);
      var node =
          new GenericContainer<>("nats:2.12.8-alpine")
              .withNetwork(network)
              .withNetworkAliases("nats" + i)
              .withExposedPorts(4222)
              .withCopyToContainer(Transferable.of(config), "/etc/nats/test.conf")
              .withCopyFileToContainer(
                  MountableFile.forHostPath(certificates.file("ca.pem")), "/certs/ca.pem")
              .withCopyFileToContainer(
                  MountableFile.forHostPath(certificates.file("server.pem")), "/certs/server.pem")
              .withCopyFileToContainer(
                  MountableFile.forHostPath(certificates.file("server.key")), "/certs/server.key")
              .withCommand("-c", "/etc/nats/test.conf")
              .waitingFor(Wait.forLogMessage(".*Server is ready.*", 1));
      brokers.add(node);
      node.start();
    }
    broker = brokers.getFirst();
    connectCluster();
    await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(
            () -> {
              nats.jetStreamManagement()
                  .addStream(
                      StreamConfiguration.builder()
                          .name(STREAM)
                          .subjects("keycloak.events.>")
                          .storageType(StorageType.File)
                          .retentionPolicy(RetentionPolicy.WorkQueue)
                          .discardPolicy(DiscardPolicy.New)
                          .replicas(3)
                          .maxBytes(16777216)
                          .duplicateWindow(Duration.ofMinutes(2))
                          .build());
              return true;
            });
    consumer();
    keycloak = tlsKeycloak(true);
    keycloak.start();
    loginAdmin();
    second = tlsKeycloak(false);
    second.start();
  }

  private static GenericContainer<?> tlsKeycloak(boolean importRealm) {
    return keycloakContainer(importRealm)
        .withEnv("KC_HOSTNAME", "http://keycloak.test")
        .withEnv("KND_NATS_URL", "tls://nats1:4222,tls://nats2:4222,tls://nats3:4222")
        .withEnv("KND_MIN_REPLICAS", "3")
        .withEnv("KND_TIMEOUT_MS", "1000")
        .withEnv("KND_TLS_CA_FILE", "/certs/ca.pem")
        .withEnv("KND_TLS_CERT_FILE", "/certs/client.pem")
        .withEnv("KND_TLS_KEY_FILE", "/certs/client.key")
        .withCopyFileToContainer(
            MountableFile.forHostPath(certificates.file("ca.pem")), "/certs/ca.pem")
        .withCopyFileToContainer(
            MountableFile.forHostPath(certificates.file("client.pem")), "/certs/client.pem")
        .withCopyFileToContainer(
            MountableFile.forHostPath(certificates.file("client.key"), 0444), "/certs/client.key");
  }

  @BeforeEach
  void reset() throws Exception {
    loginAdmin();
    drained();
    nats.jetStreamManagement().deleteConsumer(STREAM, DURABLE);
    nats.jetStreamManagement().purgeStream(STREAM);
    consumer();
  }

  private static void connectCluster() throws Exception {
    if (nats != null) {
      nats.close();
    }
    String[] urls =
        brokers.stream()
            .filter(GenericContainer::isRunning)
            .map(node -> "tls://localhost:" + currentPort(node, 4222))
            .toArray(String[]::new);
    var tls =
        new TlsConfig(
            true,
            certificates.file("ca.pem"),
            certificates.file("client.pem"),
            certificates.file("client.key"));
    nats =
        Nats.connect(
            new Options.Builder()
                .servers(urls)
                .sslContext(tls.createContext())
                .hostnameResolveMode(Options.HostnameResolveMode.HappyEyeballs)
                .ignoreDiscoveredServers()
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(100))
                .connectionTimeout(Duration.ofSeconds(2))
                .build());
  }

  @Test
  @Order(1)
  void rejectsUntrustedPeersWrongHostnamesAndMissingClientCertificates() throws Exception {
    String url = "tls://localhost:" + currentPort(broker, 4222);
    record RejectedPeer(String url, TlsConfig config) {}

    var invalid =
        List.of(
            new RejectedPeer(
                url,
                new TlsConfig(
                    true, null, certificates.file("client.pem"), certificates.file("client.key"))),
            new RejectedPeer(url, new TlsConfig(true, certificates.file("ca.pem"), null, null)),
            new RejectedPeer(
                url.replace("localhost", "127.0.0.1"),
                new TlsConfig(
                    true,
                    certificates.file("ca.pem"),
                    certificates.file("client.pem"),
                    certificates.file("client.key"))));
    for (var peer : invalid) {
      var options =
          new Options.Builder()
              .server(peer.url())
              .sslContext(peer.config().createContext())
              .hostnameResolveMode(Options.HostnameResolveMode.HappyEyeballs)
              .ignoreDiscoveredServers()
              .maxReconnects(0)
              .connectionTimeout(Duration.ofSeconds(2))
              .build();
      assertThrows(
          IOException.class,
          () -> {
            try (var unexpected = Nats.connect(options)) {
              unexpected.flush(Duration.ofSeconds(1));
            }
          });
    }
    assertEquals(0, messages());
  }

  @Test
  @Order(2)
  void leaderLossPreservesReplicatedMessagesAndConsumerAcknowledgements() throws Exception {
    assertTrue(
        second.getLogs().contains(" (2) ["),
        "Both Keycloak nodes must join the same cache cluster");
    loginAdmin();
    try (var pool = Executors.newFixedThreadPool(6)) {
      List<Future<String>> results = new ArrayList<>();
      for (int i = 0; i < 24; i++) {
        var node = i % 2 == 0 ? keycloak : second;
        results.add(pool.submit(() -> createUserOn(node)));
      }
      for (var result : results) {
        assertFalse(result.get().isBlank());
      }
    }
    drained();
    assertEquals(24, messages());
    var ids = consume(4);
    awaitMessages(20);
    String leader = nats.jetStreamManagement().getStreamInfo(STREAM).getClusterInfo().getLeader();
    var lost = brokers.get(Integer.parseInt(leader.substring(4)) - 1);
    lost.getDockerClient().killContainerCmd(lost.getContainerId()).withSignal("KILL").exec();
    try {
      connectCluster();
      await()
          .atMost(Duration.ofSeconds(30))
          .ignoreExceptions()
          .until(
              () -> {
                String elected =
                    nats.jetStreamManagement().getStreamInfo(STREAM).getClusterInfo().getLeader();
                return elected != null && !elected.isBlank() && !leader.equals(elected);
              });
      for (String id : consume(20)) {
        assertTrue(ids.add(id), "An acknowledged event must not return after leader failover");
      }
      assertEquals(24, ids.size());
      createUserOn(second);
      drained();
      assertEquals(1, consume(1).size());
      awaitMessages(0);
    } finally {
      lost.getDockerClient().startContainerCmd(lost.getContainerId()).exec();
      connectCluster();
      awaitReplicas();
    }
  }

  @Test
  @Order(3)
  void quorumLossRetainsOutboxAndSurvivingKeycloakDrainsAfterRecovery() throws Exception {
    loginAdmin();
    awaitReplicas();
    for (int i = 0; i < 2; i++) {
      var node = brokers.get(i);
      node.getDockerClient().killContainerCmd(node.getContainerId()).withSignal("KILL").exec();
    }
    try {
      createUser();
      createUserOn(second);
      createUser();
      await()
          .atMost(Duration.ofSeconds(15))
          .until(() -> scalar("SELECT count(*) FROM kc_nats_outbox WHERE attempts > 0") == 3);
      keycloak
          .getDockerClient()
          .killContainerCmd(keycloak.getContainerId())
          .withSignal("KILL")
          .exec();
      var expected = new HashSet<String>();
      try (var db = database.getConnection();
          var statement = db.createStatement();
          var rows = statement.executeQuery("SELECT id FROM kc_nats_outbox")) {
        while (rows.next()) {
          expected.add(rows.getString(1));
        }
      }
      assertEquals(3, expected.size());
      for (int i = 0; i < 2; i++) {
        var node = brokers.get(i);
        node.getDockerClient().startContainerCmd(node.getContainerId()).exec();
      }
      connectCluster();
      drained();
      assertEquals(expected, consume(3));
      awaitMessages(0);
    } finally {
      for (var node : brokers) {
        if (!node.isRunning()) {
          node.getDockerClient().startContainerCmd(node.getContainerId()).exec();
        }
      }
    }
  }

  private static HashSet<String> consume(int count) throws Exception {
    // Stream, metadata and durable-consumer Raft groups elect leaders independently.
    await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(
            () -> {
              var cluster =
                  nats.jetStreamManagement().getConsumerInfo(STREAM, DURABLE).getClusterInfo();
              return cluster != null
                  && cluster.getLeader() != null
                  && !cluster.getLeader().isBlank()
                  && cluster.getReplicas().size() == 2;
            });
    var subscription = nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
    try {
      var messages = subscription.fetch(count, Duration.ofSeconds(10));
      assertEquals(count, messages.size());
      var ids = new HashSet<String>();
      for (var message : messages) {
        assertTrue(ids.add(objectMapper.readTree(message.getData()).path("id").asText()));
        message.ackSync(Duration.ofSeconds(3));
      }
      return ids;
    } finally {
      subscription.unsubscribe();
    }
  }

  private static void awaitReplicas() {
    await()
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(
            () -> {
              var replicas =
                  nats.jetStreamManagement().getStreamInfo(STREAM).getClusterInfo().getReplicas();
              return replicas.size() == 2
                  && replicas.stream().allMatch(replica -> replica.isCurrent());
            });
  }

  private static void awaitMessages(long count) {
    // A confirmed consumer ACK can precede the replicated stream's WorkQueue removal.
    await().atMost(Duration.ofSeconds(10)).until(() -> messages() == count);
  }

  @AfterAll
  static void stop() throws Exception {
    try {
      for (int i = 0; i < brokers.size(); i++) {
        var node = brokers.get(i);
        if (node.getContainerId() != null) {
          Files.writeString(Path.of("target/nats-cluster-" + (i + 1) + ".log"), node.getLogs());
        }
      }
      if (second != null && second.getContainerId() != null) {
        Files.writeString(Path.of("target/keycloak-tls-second.log"), second.getLogs());
      }
    } finally {
      if (second != null) {
        second.stop();
      }
      for (var node : brokers) {
        if (node != broker) {
          node.stop();
        }
      }
      stopInfrastructure();
    }
  }
}
