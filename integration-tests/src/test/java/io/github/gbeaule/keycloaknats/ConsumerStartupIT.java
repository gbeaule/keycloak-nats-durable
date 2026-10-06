package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gbeaule.keycloaknats.consumer.ConsumerMain;
import io.github.gbeaule.keycloaknats.consumer.InboxProcessor;
import io.github.gbeaule.keycloaknats.consumer.ProcessingLimits;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/** Executes the consumer entry point against live broker configuration before it can ACK. */
@Testcontainers
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class ConsumerStartupIT {
  private static final String DURABLE = "auth-worker";
  private static final String SUBJECT = "keycloak.events.realm.user.login";

  @Container
  private static final PostgreSQLContainer postgres =
      IntegrationSupport.postgresContainer()
          .withDatabaseName("consumer")
          .withUsername("consumer")
          .withPassword("integration-only");

  @Container
  private static final GenericContainer<?> broker =
      new GenericContainer<>(IntegrationSupport.NATS_IMAGE)
          .withExposedPorts(4222)
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("nats.conf"), "/etc/nats/test.conf")
          .withCommand("-c", "/etc/nats/test.conf");

  private static PGSimpleDataSource database;
  private static Connection nats;

  record Scenario(
      String name, List<String> streamSubjects, List<String> filters, boolean accepted) {
    @Override
    public String toString() {
      return name;
    }
  }

  @BeforeAll
  static void start() throws Exception {
    database = new PGSimpleDataSource();
    database.setURL(postgres.getJdbcUrl());
    database.setUser(postgres.getUsername());
    database.setPassword(postgres.getPassword());
    ProcessingLimits.defaults().configure(database);
    InboxProcessor.initialize(database);
    nats = Nats.connect("nats://" + broker.getHost() + ":" + broker.getMappedPort(4222));
  }

  @AfterAll
  static void stop() throws Exception {
    if (nats != null) {
      nats.close();
    }
  }

  @BeforeEach
  void reset() throws Exception {
    try (var db = database.getConnection();
        var query = db.createStatement()) {
      query.execute("TRUNCATE knd_inbox,knd_effects,knd_quarantine");
    }
  }

  static Stream<Scenario> namespaces() {
    var own = List.of("keycloak.events.>");
    var mixed = List.of("keycloak.events.>", "other.events.>");
    return Stream.of(
        new Scenario("filtered subset", own, List.of("keycloak.events.*.user.login"), true),
        new Scenario(
            "multiple filtered subsets",
            mixed,
            List.of("keycloak.events.*.user.login", "keycloak.events.*.admin.user.update"),
            true),
        new Scenario("unfiltered bounded stream", own, List.of(), true),
        new Scenario("wrong single filter", mixed, List.of("other.events.>"), false),
        new Scenario(
            "one wrong filter among several",
            mixed,
            List.of("keycloak.events.*.user.login", "other.events.>"),
            false),
        new Scenario("unfiltered mixed stream", mixed, List.of(), false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("namespaces")
  void startupChecksTheEffectiveSubjectNamespace(Scenario scenario) throws Exception {
    String stream = "STARTUP_" + UUID.randomUUID().toString().replace("-", "");
    nats.jetStreamManagement()
        .addStream(
            StreamConfiguration.builder()
                .name(stream)
                .subjects(scenario.streamSubjects())
                .storageType(StorageType.File)
                .retentionPolicy(RetentionPolicy.Limits)
                .discardPolicy(DiscardPolicy.New)
                .replicas(1)
                .build());
    nats.jetStreamManagement()
        .createConsumer(
            stream,
            ConsumerConfiguration.builder()
                .durable(DURABLE)
                .filterSubjects(scenario.filters())
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.All)
                .maxDeliver(-1)
                .ackWait(Duration.ofSeconds(30))
                .build());
    nats.jetStream()
        .publish(
            SUBJECT,
            new ObjectMapper()
                .writeValueAsBytes(
                    Map.of(
                        "specversion", "1.0",
                        "id", UUID.randomUUID(),
                        "source", "urn:keycloak:realm:realm",
                        "type", "io.keycloak.user.login",
                        "dataschema", "urn:keycloak-nats:event:v1",
                        "data", Map.of())));

    Process consumer = launch(stream);
    var output = new ByteArrayOutputStream();
    Thread reader =
        Thread.ofVirtual()
            .start(
                () -> {
                  try (var input = consumer.getInputStream()) {
                    input.transferTo(output);
                  } catch (IOException closed) {
                    // Test cleanup can close the pipe while the process is terminating.
                  }
                });
    try {
      if (scenario.accepted()) {
        await()
            .atMost(Duration.ofSeconds(20))
            .until(() -> output.toString(StandardCharsets.UTF_8).contains("Consumer running;"));
        assertTrue(consumer.isAlive(), output.toString(StandardCharsets.UTF_8));
        await().atMost(Duration.ofSeconds(10)).until(() -> effects() == 1);
      } else {
        assertTrue(consumer.waitFor(20, TimeUnit.SECONDS), "Consumer must reject startup");
        reader.join(5000);
        assertEquals(1, consumer.exitValue(), output.toString(StandardCharsets.UTF_8));
        assertFalse(output.toString(StandardCharsets.UTF_8).contains("Consumer running;"));
        assertTrue(
            output
                .toString(StandardCharsets.UTF_8)
                .contains("failure category=IllegalStateException"));
        assertEquals(0, effects());
        assertEquals(
            0,
            nats.jetStreamManagement()
                .getConsumerInfo(stream, DURABLE)
                .getDelivered()
                .getStreamSequence(),
            "Namespace rejection must precede the subscription and any delivery");
      }
    } finally {
      consumer.destroy();
      if (!consumer.waitFor(5, TimeUnit.SECONDS)) {
        consumer.destroyForcibly();
        consumer.waitFor(5, TimeUnit.SECONDS);
      }
      consumer.getInputStream().close();
      consumer.getOutputStream().close();
      reader.join(5000);
      Files.writeString(
          Path.of("target", "consumer-startup-" + scenario.name().replace(' ', '-') + ".log"),
          output.toString(StandardCharsets.UTF_8));
      nats.jetStreamManagement().deleteStream(stream);
    }
  }

  private Process launch(String stream) throws Exception {
    boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
    String java =
        Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java").toString();
    var process =
        new ProcessBuilder(
                java,
                "-cp",
                System.getProperty(
                    "surefire.test.class.path", System.getProperty("java.class.path")),
                ConsumerMain.class.getName(),
                "run")
            .redirectErrorStream(true);
    var environment = process.environment();
    environment.keySet().removeIf(key -> key.toUpperCase(Locale.ROOT).startsWith("KND_"));
    environment.put(
        "KND_NATS_URL", "nats://" + broker.getHost() + ":" + broker.getMappedPort(4222));
    environment.put("KND_STREAM", stream);
    environment.put("KND_CONSUMER", DURABLE);
    environment.put("KND_SUBJECT_PREFIX", "keycloak.events");
    environment.put("KND_CONSUMER_DB_URL", postgres.getJdbcUrl());
    environment.put("KND_CONSUMER_DB_USER", postgres.getUsername());
    environment.put("KND_CONSUMER_DB_PASSWORD", postgres.getPassword());
    return process.start();
  }

  private long effects() throws Exception {
    try (var db = database.getConnection();
        var query = db.createStatement();
        var result = query.executeQuery("SELECT count(*) FROM knd_effects")) {
      result.next();
      return result.getLong(1);
    }
  }
}
