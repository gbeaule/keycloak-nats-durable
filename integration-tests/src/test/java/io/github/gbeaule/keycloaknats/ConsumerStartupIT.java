package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gbeaule.keycloaknats.consumer.FailurePolicy;
import io.github.gbeaule.keycloaknats.consumer.InboxProcessor;
import io.github.gbeaule.keycloaknats.consumer.ProcessingLimits;
import io.github.gbeaule.keycloaknats.consumer.QuarantineStore;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.PersistMode;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  @Test
  void replayRejectsAsyncPersistenceAndRetainsTheCopyUntilDefaultPersistenceAcceptsIt()
      throws Exception {
    String stream = "REPLAY_" + UUID.randomUUID().toString().replace("-", "");
    String id = UUID.randomUUID().toString();
    byte[] payload =
        new ObjectMapper().writeValueAsBytes(Map.of("id", id, "data", Map.of("original", true)));
    var quarantine = new QuarantineStore(database, DURABLE, ProcessingLimits.defaults());
    try {
      replayStream(stream, PersistMode.Default);
      nats.jetStreamManagement()
          .createConsumer(
              stream,
              ConsumerConfiguration.builder()
                  .durable(DURABLE)
                  .ackPolicy(AckPolicy.Explicit)
                  .build());
      nats.jetStream().publish(SUBJECT, payload, PublishOptions.builder().messageId(id).build());
      var subscription =
          nats.jetStream().subscribe(null, PullSubscribeOptions.bind(stream, DURABLE));
      try {
        var messages = subscription.fetch(1, Duration.ofSeconds(5));
        assertEquals(1, messages.size());
        var message = messages.getFirst();
        quarantine.record(message, FailurePolicy.Action.QUARANTINE, "UNSUPPORTED_ENVELOPE");
        message.ackSync(Duration.ofSeconds(2));
      } finally {
        subscription.unsubscribe();
      }
      assertEquals(
          0, nats.jetStreamManagement().getStreamInfo(stream).getStreamState().getMsgCount());
      assertEquals(1, quarantine.pending(10).size());

      // Persist mode cannot be updated in place. Recreate the empty stream to test each policy.
      nats.jetStreamManagement().deleteStream(stream);
      replayStream(stream, PersistMode.Async);
      assertEquals(
          PersistMode.Async,
          nats.jetStreamManagement().getStreamInfo(stream).getConfiguration().getPersistMode());
      assertTrue(replay(stream, 1).contains("failure category=Violation"));
      assertEquals(
          1, quarantine.pending(10).size(), "Rejected replay must keep its recovery copy pending");
      assertEquals(
          0, nats.jetStreamManagement().getStreamInfo(stream).getStreamState().getMsgCount());

      nats.jetStreamManagement().deleteStream(stream);
      replayStream(stream, PersistMode.Default);
      assertTrue(replay(stream, 0).contains("Replay published;"));
      assertTrue(quarantine.pending(10).isEmpty());
      assertEquals(
          1, nats.jetStreamManagement().getStreamInfo(stream).getStreamState().getMsgCount());
      var restored = nats.jetStreamManagement().getMessage(stream, 1);
      assertArrayEquals(payload, restored.getData());
      assertEquals(id, restored.getHeaders().getFirst("Nats-Msg-Id"));
    } finally {
      nats.jetStreamManagement().deleteStream(stream);
    }
  }

  private void replayStream(String stream, PersistMode mode) throws Exception {
    nats.jetStreamManagement()
        .addStream(
            StreamConfiguration.builder()
                .name(stream)
                .subjects("keycloak.events.>")
                .storageType(StorageType.File)
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .replicas(1)
                .persistMode(mode)
                .duplicateWindow(Duration.ofMinutes(2))
                .build());
  }

  private String replay(String stream, int expectedExit) throws Exception {
    Process consumer = launch(stream, "quarantine-replay", stream, "1");
    try {
      assertTrue(
          consumer.waitFor(20, TimeUnit.SECONDS), "Replay must complete or reject the destination");
      String output = new String(consumer.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      Files.writeString(Path.of("target", "consumer-replay-exit-" + expectedExit + ".log"), output);
      assertEquals(expectedExit, consumer.exitValue(), output);
      return output;
    } finally {
      consumer.destroyForcibly();
      consumer.waitFor(5, TimeUnit.SECONDS);
    }
  }

  private Process launch(String stream, String... arguments) throws Exception {
    boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
    String java =
        Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java").toString();
    // Use the shipped executable; the test classpath also contains the provider's relocated NATS.
    String artifact = System.getProperty("consumer.jar");
    assertTrue(Files.isRegularFile(Path.of(artifact)), "Package the consumer before its CLI tests");
    var command = new ArrayList<>(List.of(java, "-jar", artifact));
    command.addAll(arguments.length == 0 ? List.of("run") : List.of(arguments));
    var process = new ProcessBuilder(command).redirectErrorStream(true);
    var environment = process.environment();
    environment.keySet().removeIf(key -> key.toUpperCase(Locale.ROOT).startsWith("KND_"));
    environment.put(
        "KND_NATS_URL", "nats://" + broker.getHost() + ":" + broker.getMappedPort(4222));
    environment.put("KND_STREAM", stream);
    environment.put("KND_CONSUMER", DURABLE);
    environment.put("KND_SUBJECT_PREFIX", "keycloak.events");
    environment.put("KND_MIN_REPLICAS", "1");
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
