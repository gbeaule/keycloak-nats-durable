package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gbeaule.keycloaknats.consumer.InboxProcessor;
import io.github.gbeaule.keycloaknats.consumer.ProcessingLimits;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

/** Exercises reconnect and shutdown in the shipped Java 21 process without a restart supervisor. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class ConsumerReconnectIT {
  private static final String STREAM = "RECONNECT_EVENTS";
  private static final String DURABLE = "reconnect-worker";
  private static final String SUBJECT = "keycloak.events.realm.user.login";
  private static final int HEALTH_PORT = 8081;
  private static final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

  @Test
  void packagedConsumerSurvivesBrokerOutageAndResumesWithoutProcessRestart() throws Exception {
    try (var network = Network.newNetwork();
        var postgres =
            IntegrationSupport.postgresContainer()
                .withDatabaseName("consumer")
                .withUsername("consumer")
                .withPassword("integration-only")
                .withNetwork(network)
                .withNetworkAliases("postgres");
        var broker =
            new GenericContainer<>(IntegrationSupport.NATS_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("nats")
                .withExposedPorts(4222)
                .withCopyFileToContainer(
                    MountableFile.forClasspathResource("nats.conf"), "/etc/nats/test.conf")
                .withCommand("-c", "/etc/nats/test.conf")) {
      postgres.start();
      broker.start();
      var database = new PGSimpleDataSource();
      database.setURL(postgres.getJdbcUrl());
      database.setUser(postgres.getUsername());
      database.setPassword(postgres.getPassword());
      ProcessingLimits.defaults().configure(database);
      InboxProcessor.initialize(database);
      try (Connection publisher = connectBroker(broker)) {
        provision(publisher);
      }

      try (var consumer =
          new GenericContainer<>(consumerImage())
              .withNetwork(network)
              .withExposedPorts(HEALTH_PORT)
              .withEnv("KND_NATS_URL", "nats://nats:4222")
              .withEnv("KND_CONSUMER_DB_URL", "jdbc:postgresql://postgres:5432/consumer")
              .withEnv("KND_CONSUMER_DB_USER", postgres.getUsername())
              .withEnv("KND_CONSUMER_DB_PASSWORD", postgres.getPassword())
              .withEnv("KND_STREAM", STREAM)
              .withEnv("KND_CONSUMER", DURABLE)
              .withEnv("KND_MIN_REPLICAS", "1")
              .withEnv("KND_CONSUMER_HEALTH_BIND", "0.0.0.0")
              .withEnv("KND_CONSUMER_HEALTH_PORT", Integer.toString(HEALTH_PORT))
              .withEnv("KND_CONSUMER_MONITOR_SECONDS", "1")
              .waitingFor(Wait.forHttp("/health/ready").forStatusCode(200))) {
        try {
          consumer.start();
          var javaVersion = consumer.execInContainer("java", "-version");
          assertEquals(0, javaVersion.getExitCode());
          assertTrue(javaVersion.getStderr().contains("version \"21."), javaVersion.getStderr());
          String startedAt = inspect(consumer).getState().getStartedAt();
          try (Connection publisher = connectBroker(broker)) {
            publish(publisher);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertEffects(database, 1));
          }

          broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
          // Exceed multiple two-second pulls. The original process exited on its first rejection.
          await()
              .during(Duration.ofSeconds(6))
              .atMost(Duration.ofSeconds(20))
              .untilAsserted(
                  () -> {
                    assertSameProcess(consumer, startedAt);
                    assertReadiness(consumer, false);
                    assertEffects(database, 1);
                  });

          broker.getDockerClient().startContainerCmd(broker.getContainerId()).exec();
          try (Connection publisher = connectBroker(broker)) {
            publish(publisher);
            await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(
                    () -> {
                      assertSameProcess(consumer, startedAt);
                      assertReadiness(consumer, true);
                      assertEffects(database, 2);
                    });
          }

          // SIGTERM while reconnecting must interrupt the pull backoff instead of requiring
          // SIGKILL.
          broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
          await()
              .atMost(Duration.ofSeconds(10))
              .untilAsserted(() -> assertReadiness(consumer, false));
          consumer
              .getDockerClient()
              .stopContainerCmd(consumer.getContainerId())
              .withTimeout(5)
              .exec();
          long exitCode = inspect(consumer).getState().getExitCodeLong();
          assertTrue(exitCode == 0 || exitCode == 143, "Graceful JVM SIGTERM exit: " + exitCode);
        } finally {
          if (consumer.getContainerId() != null) {
            Files.createDirectories(Path.of("target"));
            Files.writeString(Path.of("target/consumer-reconnect.log"), consumer.getLogs());
          }
        }
      }
    }
  }

  private static String consumerImage() {
    Path artifact = Path.of(System.getProperty("consumer.jar")).toAbsolutePath();
    String version =
        artifact.getFileName().toString().replace("consumer-example-", "").replace(".jar", "");
    Path repository = artifact.getParent().getParent().getParent();
    return new ImageFromDockerfile("knd-consumer-reconnect-test:" + UUID.randomUUID(), true)
        .withFileFromPath(
            "deploy/Dockerfile.consumer", repository.resolve("deploy/Dockerfile.consumer"))
        .withFileFromPath("consumer-example/target/" + artifact.getFileName(), artifact)
        .withDockerfilePath("deploy/Dockerfile.consumer")
        .withBuildArg("ARTIFACT_VERSION", version)
        .get();
  }

  private static Connection connectBroker(GenericContainer<?> broker) {
    var connected = new AtomicReference<Connection>();
    await("own broker accepts connections")
        .atMost(Duration.ofSeconds(20))
        .ignoreExceptionsInstanceOf(IOException.class)
        .untilAsserted(
            () ->
                connected.set(
                    Nats.connect(
                        new Options.Builder()
                            .server(
                                "nats://"
                                    + broker.getHost()
                                    + ":"
                                    + IntegrationSupport.currentPort(broker, 4222))
                            .connectionTimeout(Duration.ofSeconds(2))
                            .build())));
    return connected.get();
  }

  private static void provision(Connection publisher) throws Exception {
    publisher
        .jetStreamManagement()
        .addStream(
            StreamConfiguration.builder()
                .name(STREAM)
                .subjects("keycloak.events.>")
                .storageType(StorageType.File)
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .replicas(1)
                .build());
    publisher
        .jetStreamManagement()
        .createConsumer(
            STREAM,
            ConsumerConfiguration.builder()
                .durable(DURABLE)
                .filterSubject("keycloak.events.>")
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.All)
                .maxDeliver(-1)
                .ackWait(Duration.ofSeconds(30))
                .build());
  }

  private static void publish(Connection publisher) throws Exception {
    byte[] event =
        new ObjectMapper()
            .writeValueAsBytes(
                Map.of(
                    "specversion", "1.0",
                    "id", UUID.randomUUID().toString(),
                    "source", "urn:keycloak:realm:realm",
                    "type", "io.keycloak.user.login",
                    "dataschema", "urn:keycloak-nats:event:v1",
                    "data", Map.of()));
    publisher.jetStream().publish(SUBJECT, event);
  }

  private static com.github.dockerjava.api.command.InspectContainerResponse inspect(
      GenericContainer<?> consumer) {
    return consumer.getDockerClient().inspectContainerCmd(consumer.getContainerId()).exec();
  }

  private static void assertSameProcess(GenericContainer<?> consumer, String startedAt) {
    var state = inspect(consumer);
    assertTrue(Boolean.TRUE.equals(state.getState().getRunning()), consumer.getLogs());
    assertEquals(startedAt, state.getState().getStartedAt());
    assertEquals(Integer.valueOf(0), state.getRestartCount());
  }

  private static void assertReadiness(GenericContainer<?> consumer, boolean ready)
      throws Exception {
    var response =
        http.send(
            HttpRequest.newBuilder(
                    URI.create(
                        "http://"
                            + consumer.getHost()
                            + ":"
                            + consumer.getMappedPort(HEALTH_PORT)
                            + "/health/ready"))
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(ready ? 200 : 503, response.statusCode());
    assertEquals("{\"ready\":" + ready + "}\n", response.body());
  }

  private static void assertEffects(PGSimpleDataSource database, int expected) throws Exception {
    try (var db = database.getConnection();
        var statement = db.createStatement();
        var rows = statement.executeQuery("SELECT count(*) FROM knd_effects")) {
      assertTrue(rows.next());
      assertEquals(expected, rows.getInt(1));
    }
  }
}
