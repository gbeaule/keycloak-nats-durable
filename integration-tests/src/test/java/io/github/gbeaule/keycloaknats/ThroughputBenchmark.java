package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.gbeaule.keycloaknats.consumer.ConsumerDatabase;
import io.github.gbeaule.keycloaknats.consumer.InboxProcessor;
import io.github.gbeaule.keycloaknats.consumer.ProcessingLimits;
import io.nats.client.PullSubscribeOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/** Opt-in repeatable load measurement; does not turn a workstation result into a production SLO. */
class ThroughputBenchmark extends IntegrationSupport {
  @TestFactory
  Stream<DynamicTest> measureRelayConcurrency() {
    return Arrays.stream(System.getProperty("benchmark.relay.workers", "1,4").split(","))
        .map(String::strip)
        .mapToInt(Integer::parseInt)
        .distinct()
        .mapToObj(
            workers -> DynamicTest.dynamicTest("relay-workers-" + workers, () -> measure(workers)));
  }

  private void measure(int workers) throws Exception {
    if (workers < 1 || workers > 16) {
      throw new IllegalArgumentException("benchmark.relay.workers must contain values 1..16");
    }
    int events = setting("benchmark.events", 500, 20, 100000);
    int concurrency = setting("benchmark.concurrency", 8, 1, 128);
    int warmup = setting("benchmark.warmup", 20, 2, 10000);
    var report = new LinkedHashMap<String, Object>();
    try {
      startInfrastructure(
          System.getProperty("keycloak.version"),
          container ->
              container
                  .withEnv("KND_RELAY_WORKERS", Integer.toString(workers))
                  .withEnv("KC_DB_POOL_MAX_SIZE", "32")
                  .withCommand(
                      "start",
                      "--http-enabled=true",
                      "--hostname=http://localhost:8080",
                      "--import-realm",
                      "--cache=ispn"));
      InboxProcessor.initialize(database);
      assertEquals("on", databaseSetting("fsync"));
      assertEquals("on", databaseSetting("synchronous_commit"));
      report.put("timestamp", Instant.now().toString());
      report.put("keycloakVersion", System.getProperty("keycloak.version"));
      report.put("postgresImage", POSTGRES_IMAGE);
      report.put("javaVersion", System.getProperty("java.version"));
      report.put("clientOperatingSystem", System.getProperty("os.name"));
      report.put("clientAvailableProcessors", Runtime.getRuntime().availableProcessors());
      report.put("relayWorkers", workers);
      report.put("requestConcurrency", concurrency);
      report.put("keycloakDbPoolMax", 32);
      report.put("eventsPerPhase", events);
      report.put("warmupRequests", warmup);
      report.put(
          "storage", "PostgreSQL fsync/synchronous_commit on; one NATS File replica, sync always");
      report.put(
          "workload", "Alternating password login and admin user creation; isolated containers");
      requestLoad(warmup, concurrency);
      drained();
      consume(warmup, Set.of());

      long liveStart = System.nanoTime();
      Map<String, Object> live = requestLoad(events, concurrency);
      drained();
      live.put("captureAndRelaySeconds", secondsSince(liveStart));
      live.put("captureAndRelayEventsPerSecond", events / secondsSince(liveStart));
      live.put("consumer", consume(events, Set.of()));
      report.put("live", live);

      var docker = broker.getDockerClient();
      docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
      try {
        Map<String, Object> outage = requestLoad(events, concurrency);
        assertEquals(events, scalar("SELECT count(*) FROM kc_nats_outbox"));
        Set<String> accepted = pendingIds();
        outage.put("pendingRows", accepted.size());
        outage.put("tableBytes", scalar("SELECT pg_total_relation_size('kc_nats_outbox')"));
        outage.put("payloadBytes", scalar("SELECT sum(octet_length(payload)) FROM kc_nats_outbox"));
        report.put("outageCapture", outage);

        final long recoveryStart = System.nanoTime();
        docker.startContainerCmd(broker.getContainerId()).exec();
        connectNats();
        int ongoing = Math.max(20, events / 2);
        final Map<String, Object> duringRecovery = requestLoad(ongoing, concurrency);
        await()
            .atMost(Duration.ofMinutes(5))
            .until(() -> scalar("SELECT count(*) FROM kc_nats_outbox") == 0);
        double recoverySeconds = secondsSince(recoveryStart);
        var recovery = new LinkedHashMap<String, Object>();
        recovery.put("initialBacklog", events);
        recovery.put("additionalCapture", ongoing);
        recovery.put("restartAndDrainSeconds", recoverySeconds);
        recovery.put("confirmedEventsPerSecond", (events + ongoing) / recoverySeconds);
        recovery.put("requestsWhileDraining", duringRecovery);
        recovery.put("consumer", consume(events + ongoing, accepted));
        report.put("recovery", recovery);
      } finally {
        if (!broker.isRunning()) {
          docker.startContainerCmd(broker.getContainerId()).exec();
        }
      }
      Path output = Path.of("target", "performance-workers-" + workers + ".json");
      Files.createDirectories(output.getParent());
      Files.writeString(
          output, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(report));
      System.out.println("Performance report: " + output.toAbsolutePath());
      System.out.println(objectMapper.writeValueAsString(report));
    } finally {
      if (keycloak != null && keycloak.getContainerId() != null) {
        Files.writeString(
            Path.of("target", "performance-workers-" + workers + "-keycloak.log"),
            keycloak.getLogs());
      }
      stopInfrastructure();
    }
  }

  private Map<String, Object> requestLoad(int requests, int concurrency) throws Exception {
    var logins = new ConcurrentLinkedQueue<Double>();
    var administration = new ConcurrentLinkedQueue<Double>();
    long started = System.nanoTime();
    try (var pool = Executors.newFixedThreadPool(concurrency)) {
      var work = new ArrayList<java.util.concurrent.Future<?>>();
      for (int i = 0; i < requests; i++) {
        boolean authentication = i % 2 == 0;
        work.add(
            pool.submit(
                () -> {
                  long requestStart = System.nanoTime();
                  if (authentication) {
                    assertEquals(
                        200,
                        login(
                                "durable-test",
                                "grant_type=password&client_id=test-client"
                                    + "&username=alice&password=alice-password")
                            .statusCode());
                    logins.add(secondsSince(requestStart) * 1000);
                  } else {
                    createUser();
                    administration.add(secondsSince(requestStart) * 1000);
                  }
                  return null;
                }));
      }
      for (var request : work) {
        request.get(30, TimeUnit.SECONDS);
      }
    }
    double seconds = secondsSince(started);
    var result = new LinkedHashMap<String, Object>();
    result.put("requests", requests);
    result.put("seconds", seconds);
    result.put("requestsPerSecond", requests / seconds);
    result.put("loginLatencyMs", percentiles(List.copyOf(logins)));
    result.put("adminLatencyMs", percentiles(List.copyOf(administration)));
    return result;
  }

  private Map<String, Object> consume(int expected, Set<String> originallyPending)
      throws Exception {
    try (var pool = ConsumerDatabase.pool(database, ProcessingLimits.defaults(), 1)) {
      return consume(expected, originallyPending, pool);
    }
  }

  private Map<String, Object> consume(
      int expected, Set<String> originallyPending, javax.sql.DataSource databasePool)
      throws Exception {
    var processor = new InboxProcessor(databasePool, DURABLE);
    var seen = new HashSet<String>();
    var latency = new ArrayList<Double>();
    long effectsBefore = scalar("SELECT count(*) FROM knd_effects");
    final long started = System.nanoTime();
    var subscription = nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
    try {
      while (seen.size() < expected) {
        var messages = subscription.fetch(1, Duration.ofSeconds(5));
        assertTrue(!messages.isEmpty(), "All accepted events must reach the consumer");
        for (var message : messages) {
          String id = objectMapper.readTree(message.getData()).path("id").asText();
          long processingStart = System.nanoTime();
          boolean inserted = processor.process(message.getData(), processor::recordEffect);
          assertEquals(
              seen.add(id), inserted, "Each identity must have exactly one database effect");
          message.ackSync(Duration.ofSeconds(2));
          latency.add(secondsSince(processingStart) * 1000);
        }
      }
    } finally {
      subscription.unsubscribe();
    }
    assertTrue(seen.containsAll(originallyPending), "Recovery preserves every persisted outbox ID");
    assertEquals(expected, scalar("SELECT count(*) FROM knd_effects") - effectsBefore);
    await().atMost(Duration.ofSeconds(10)).until(() -> messages() == 0);
    double seconds = secondsSince(started);
    return Map.of(
        "uniqueEffects",
        seen.size(),
        "seconds",
        seconds,
        "eventsPerSecond",
        expected / seconds,
        "databaseAndAckLatencyMs",
        percentiles(latency),
        "workers",
        1);
  }

  private static Set<String> pendingIds() throws Exception {
    var ids = new HashSet<String>();
    try (var db = database.getConnection();
        var query = db.createStatement();
        var result = query.executeQuery("SELECT id FROM kc_nats_outbox")) {
      while (result.next()) {
        ids.add(result.getString(1));
      }
    }
    return ids;
  }

  private static String databaseSetting(String name) throws Exception {
    try (var db = database.getConnection();
        var query = db.prepareStatement("SELECT current_setting(?)")) {
      query.setString(1, name);
      try (var result = query.executeQuery()) {
        result.next();
        return result.getString(1);
      }
    }
  }

  private static Map<String, Object> percentiles(List<Double> values) {
    List<Double> sorted = values.stream().sorted().toList();
    return Map.of(
        "count",
        sorted.size(),
        "p50",
        percentile(sorted, .50),
        "p95",
        percentile(sorted, .95),
        "p99",
        percentile(sorted, .99),
        "max",
        sorted.getLast());
  }

  private static double percentile(List<Double> sorted, double quantile) {
    return sorted.get(Math.max(0, (int) Math.ceil(quantile * sorted.size()) - 1));
  }

  private static double secondsSince(long started) {
    return (System.nanoTime() - started) / 1_000_000_000.0;
  }

  private static int setting(String name, int fallback, int minimum, int maximum) {
    int value = Integer.parseInt(System.getProperty(name, Integer.toString(fallback)));
    if (value < minimum || value > maximum) {
      throw new IllegalArgumentException("Invalid benchmark setting " + name);
    }
    return value;
  }
}
