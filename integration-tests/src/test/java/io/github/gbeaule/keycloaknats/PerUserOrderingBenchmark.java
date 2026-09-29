package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;

/** Opt-in source measurements using durable PostgreSQL commits and real JetStream PubAcks. */
class PerUserOrderingBenchmark extends RelayIntegrationSupport {
  private static final String OUTBOX = "\"relay-data\".kc_nats_outbox";
  private static final String AUDIT = "\"relay-data\".kc_nats_discard_audit";

  private static final class Registry extends SimpleMeterRegistry implements AutoCloseable {}

  @Test
  void measureOrderingAndDiagnosticCapacity() throws Exception {
    int events = setting("benchmark.events", 500, 20, 100000);
    int concurrency = setting("benchmark.concurrency", 8, 1, 16);
    var report = new LinkedHashMap<String, Object>();
    report.put("timestamp", Instant.now().toString());
    report.put(
        "environment",
        Map.of(
            "keycloakVersion",
            System.getProperty("keycloak.version"),
            "postgresImage",
            POSTGRES_IMAGE,
            "natsImage",
            NATS_IMAGE,
            "javaVersion",
            System.getProperty("java.version"),
            "clientOperatingSystem",
            System.getProperty("os.name"),
            "clientAvailableProcessors",
            Runtime.getRuntime().availableProcessors(),
            "docker",
            broker.getDockerClient().versionCmd().exec().toString()));
    report.put(
        "workload",
        Map.of(
            "events",
            events,
            "captureConcurrency",
            concurrency,
            "relayWorkers",
            1,
            "schema",
            "relay-data",
            "storage",
            "PostgreSQL fsync/synchronous_commit on; NATS File replica, sync always",
            "scope",
            "In-process production repositories and relay; no HTTP or consumer processing"));
    assertEquals(
        1,
        scalar(
            "SELECT (current_setting('fsync')='on'"
                + " AND current_setting('synchronous_commit')='on')::int"));
    assertTrue(nats.jetStreamManagement().getConsumerNames(STREAM).isEmpty());
    try (var publisher = new JetStreamPublisher(config)) {
      captureLoad(20, concurrency, true);
      drain(new OutboxRelay(PerUserOrderingBenchmark::transaction, publisher, config));
      report.put("hotUserCapture", captureLoad(events, concurrency, true));
      report.put("relayTransactions", relayTransactions(publisher, events));
      report.put("manyUserCapture", captureLoad(events, concurrency, false));
      drain(new OutboxRelay(PerUserOrderingBenchmark::transaction, publisher, config));
      report.put("fairnessAndRecovery", fairnessAndRecovery(publisher, events, concurrency));
      report.put("expiryAndCleanup", expiryAndCleanup(events));
    } finally {
      Path output = Path.of("target/performance-ordering.json");
      Files.createDirectories(output.getParent());
      Files.writeString(
          output, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(report));
      System.out.println("Ordering performance report: " + output.toAbsolutePath());
    }
  }

  private static Map<String, Object> captureLoad(int count, int concurrency, boolean hot)
      throws Exception {
    var allocation = new ConcurrentLinkedQueue<Double>();
    var commits = new ConcurrentLinkedQueue<Double>();
    String group = UUID.randomUUID().toString();
    var start = new CountDownLatch(1);
    long began = System.nanoTime();
    try (var executor = Executors.newFixedThreadPool(concurrency)) {
      var work = new ArrayList<java.util.concurrent.Future<?>>();
      for (int i = 0; i < count; i++) {
        String user = hot ? group : group + "-" + i;
        work.add(
            executor.submit(
                () -> {
                  start.await();
                  long before = System.nanoTime();
                  transaction(
                      em -> {
                        long allocating = System.nanoTime();
                        var ordering = CaptureRepository.next(em, "benchmark", user);
                        allocation.add(millisSince(allocating));
                        persist(em, ordering, PublicationPolicy.RETRY, 0);
                      });
                  commits.add(millisSince(before));
                  return null;
                }));
      }
      start.countDown();
      for (var future : work) {
        future.get(2, TimeUnit.MINUTES);
      }
    }
    return Map.of(
        "events",
        count,
        "seconds",
        secondsSince(began),
        "eventsPerSecond",
        count / secondsSince(began),
        "sequenceAllocationIncludingLockWaitMs",
        percentiles(List.copyOf(allocation)),
        "captureTransactionMs",
        percentiles(List.copyOf(commits)));
  }

  private static Map<String, Object> relayTransactions(JetStreamPublisher publisher, int count) {
    var intent = new ArrayList<Double>();
    var resolution = new ArrayList<Double>();
    var sends = new ArrayList<Double>();
    var single = BridgeConfig.from(Map.of("batch-size", "1"));
    for (int i = 0; i < count; i++) {
      var times = new ArrayList<Double>();
      var relay =
          new OutboxRelay(
              work -> {
                long start = System.nanoTime();
                transaction(work);
                times.add(millisSince(start));
              },
              publisher(
                  event -> {
                    long start = System.nanoTime();
                    publisher.publish(event);
                    sends.add(millisSince(start));
                  }),
              single);
      assertEquals(1, relay.runBatch().published());
      assertEquals(2, times.size(), "Intent and acknowledged resolution commit separately");
      intent.add(times.getFirst());
      resolution.add(times.getLast());
    }
    return Map.of(
        "commitsPerOriginal",
        2,
        "sendIntentTransactionMs",
        percentiles(intent),
        "pubAckAndRemovalTransactionMs",
        percentiles(resolution),
        "pubAckMs",
        percentiles(sends));
  }

  private static Map<String, Object> fairnessAndRecovery(
      JetStreamPublisher publisher, int count, int concurrency) throws Exception {
    var hot = new ArrayList<String>();
    String user = UUID.randomUUID().toString();
    transaction(
        em -> {
          for (int i = 0; i < count; i++) {
            var row =
                persist(
                    em, CaptureRepository.next(em, "benchmark", user), PublicationPolicy.RETRY, 0);
            hot.add(row.id());
            if (i == 0) {
              row.failed(Long.MAX_VALUE, "controlled backoff");
            }
            if (i % 500 == 499) {
              em.flush();
              em.clear();
            }
          }
        });
    captureLoad(concurrency, concurrency, false);
    var relay = new OutboxRelay(PerUserOrderingBenchmark::transaction, publisher, config);
    long start = System.nanoTime();
    int cold = 0;
    while (pending() > count) {
      var result = relay.runBatch();
      assertTrue(result.published() > 0, "Unrelated users must progress around the delayed head");
      cold += result.published();
    }
    final double coldMillis = millisSince(start);
    assertEquals(concurrency, cold);
    assertEquals(
        0, scalar("SELECT count(*) FROM " + OUTBOX + " WHERE publication_may_have_occurred"));
    transaction(em -> em.find(OutboxEvent.class, hot.getFirst()).failed(0, "recovery"));
    start = System.nanoTime();
    try (var executor = Executors.newSingleThreadExecutor()) {
      var captures = executor.submit(() -> captureLoad(count, concurrency, false));
      await()
          .atMost(Duration.ofMinutes(5))
          .pollInterval(Duration.ofMillis(10))
          .until(
              () -> {
                assertNotFailed(relay.runBatch());
                return captures.isDone() && pending() == 0;
              });
      double seconds = secondsSince(start);
      return Map.of(
          "delayedUserBacklog",
          count,
          "unrelatedPublished",
          cold,
          "unrelatedDrainMs",
          coldMillis,
          "recoverySeconds",
          seconds,
          "recoveryEventsPerSecond",
          2 * count / seconds,
          "captureDuringRecovery",
          captures.get(10, TimeUnit.SECONDS));
    }
  }

  private static Map<String, Object> expiryAndCleanup(int count) throws Exception {
    String user = UUID.randomUUID().toString();
    transaction(
        em -> {
          for (int i = 0; i < count; i++) {
            var row =
                persist(
                    em, CaptureRepository.next(em, "benchmark", user), PublicationPolicy.RETRY, 0);
            row.failed(Long.MAX_VALUE, "protected backlog");
            if (i % 500 == 499) {
              em.flush();
              em.clear();
            }
          }
        });
    seedExpired(count, user);
    execute("ANALYZE " + OUTBOX);
    var report = new LinkedHashMap<String, Object>();
    report.put("expiryPlan", expiryPlan());
    var relay =
        new OutboxRelay(
            PerUserOrderingBenchmark::transaction,
            publisher(
                event -> {
                  throw new AssertionError("Protected head was bypassed");
                }),
            config);
    long start = System.nanoTime();
    int expired = 0;
    while (pending() > count) {
      var result = relay.runBatch();
      assertNotFailed(result);
      assertTrue(result.expired() > 0);
      expired += result.expired();
    }
    assertEquals(count, expired);
    report.put("expiryRowsPerSecond", expired / secondsSince(start));
    long counters = scalar("SELECT count(*) FROM \"relay-data\".kc_nats_capture_counter");
    int batch = Math.min(500, count);
    var cleanupConfig =
        BridgeConfig.from(
            Map.of(
                "audit-retention-seconds",
                "0",
                "audit-cleanup-batch-size",
                Integer.toString(batch),
                "audit-cleanup-max-batches",
                "1",
                "audit-cleanup-interval-ms",
                "1000"));
    try (var registry = new Registry();
        var cleanup =
            new AuditCleanup(
                PerUserOrderingBenchmark::transaction, cleanupConfig.auditCleanup(), registry)) {
      start = System.nanoTime();
      int sweeps = 0;
      while (audits() > 0) {
        long before = audits();
        cleanup.run();
        long deleted = before - audits();
        assertTrue(deleted > 0 && deleted <= batch);
        sweeps++;
      }
      report.put("cleanupCatchUpRowsPerSecond", count / secondsSince(start));
      report.put("cleanupCatchUpSweeps", sweeps);

      // Feed less than one scheduled sweep per second, independently of cleanup completion.
      int incoming = Math.max(1, batch / 2);
      var samples = new ArrayList<Map<String, Long>>();
      start = System.nanoTime();
      cleanup.start();
      try (var executor = Executors.newSingleThreadExecutor()) {
        var traffic =
            executor.submit(
                () -> {
                  for (int round = 0; round < 12; round++) {
                    final long roundStart = System.nanoTime();
                    seedExpired(incoming, user);
                    int discarded = 0;
                    while (discarded < incoming) {
                      var result = relay.runBatch();
                      assertTrue(result.expired() > 0);
                      discarded += result.expired();
                    }
                    await()
                        .atMost(Duration.ofSeconds(2))
                        .pollInterval(Duration.ofMillis(50))
                        .until(() -> secondsSince(roundStart) >= 1);
                  }
                  return null;
                });
        await()
            .atMost(Duration.ofMinutes(5))
            .pollInterval(Duration.ofMillis(100))
            .until(
                () -> {
                  long retained = audits();
                  samples.add(
                      Map.of(
                          "retainedRows",
                          retained,
                          "relationBytes",
                          scalar("SELECT pg_total_relation_size('" + AUDIT + "')")));
                  assertTrue(
                      retained <= 2L * batch,
                      "Cleanup must keep up with the paced discard workload");
                  return traffic.isDone();
                });
        traffic.get(10, TimeUnit.SECONDS);
      }
      await().atMost(Duration.ofSeconds(10)).until(() -> audits() == 0);
      report.put(
          "sustainedDiscard",
          Map.of(
              "samples",
              samples,
              "sampleIntervalMs",
              100,
              "rowsPerRound",
              incoming,
              "incomingRowsPerSecond",
              12 * incoming / secondsSince(start),
              "configuredCleanupRowsPerSecond",
              batch,
              "finalRetainedRows",
              audits()));
      assertEquals(0, registry.get("knd.audit.cleanup.failures").counter().count());
    }
    assertEquals(count, pending(), "Audit retention must not drain protected originals");
    assertEquals(counters, scalar("SELECT count(*) FROM \"relay-data\".kc_nats_capture_counter"));
    report.put("protectedOutboxRows", pending());
    return report;
  }

  private static Object expiryPlan() throws Exception {
    long now = scalar("SELECT floor(extract(epoch FROM clock_timestamp())*1000)");
    try (var connection = database.getConnection();
        var query =
            connection.prepareStatement(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM "
                    + OUTBOX
                    + " WHERE next_expiry_attempt_at <= ?"
                    + " ORDER BY next_expiry_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED")) {
      query.setLong(1, now);
      try (var rows = query.executeQuery()) {
        assertTrue(rows.next());
        return objectMapper.readTree(rows.getString(1));
      }
    }
  }

  private static void seedExpired(int count, String user) {
    transaction(
        em -> {
          for (int i = 0; i < count; i++) {
            persist(
                em,
                CaptureRepository.next(em, "benchmark", user),
                new PublicationPolicy(1, null),
                2000);
            if (i % 500 == 499) {
              em.flush();
              em.clear();
            }
          }
        });
  }

  private static OutboxEvent persist(
      EntityManager em, EventOrdering ordering, PublicationPolicy policy, long ageMillis) {
    var event = new Event();
    event.setRealmId(ordering.realmId());
    event.setUserId(ordering.userId());
    event.setType(EventType.LOGIN);
    var envelope = new EventEnvelope(config);
    var row =
        envelope.serialize(
            envelope.describe(event),
            ordering,
            CaptureRepository.databaseTime(em) - ageMillis,
            new ResolvedPublicationPolicy(policy, EventFilter.all().sha256(), null));
    em.persist(row);
    return row;
  }

  private static void drain(OutboxRelay relay) {
    await()
        .atMost(Duration.ofMinutes(5))
        .pollInterval(Duration.ofMillis(10))
        .until(
            () -> {
              assertNotFailed(relay.runBatch());
              return pending() == 0;
            });
  }

  private static void assertNotFailed(OutboxRelay.BatchResult result) {
    assertTrue(result.outcome() != OutboxRelay.Outcome.TRANSACTION_FAILED);
    assertEquals(0, result.retries());
  }

  private static long pending() throws Exception {
    return scalar("SELECT count(*) FROM " + OUTBOX);
  }

  private static long audits() throws Exception {
    return scalar("SELECT count(*) FROM " + AUDIT);
  }

  private static Map<String, Object> percentiles(List<Double> values) {
    var sorted = values.stream().sorted().toList();
    return Map.of(
        "count",
        sorted.size(),
        "p50",
        percentile(sorted, .5),
        "p95",
        percentile(sorted, .95),
        "p99",
        percentile(sorted, .99),
        "max",
        sorted.getLast());
  }

  private static double percentile(List<Double> values, double quantile) {
    return values.get(Math.max(0, (int) Math.ceil(quantile * values.size()) - 1));
  }

  private static double secondsSince(long start) {
    return (System.nanoTime() - start) / 1_000_000_000.0;
  }

  private static double millisSince(long start) {
    return secondsSince(start) * 1000;
  }

  private static int setting(String name, int fallback, int min, int max) {
    int value = Integer.parseInt(System.getProperty(name, Integer.toString(fallback)));
    if (value < min || value > max) {
      throw new IllegalArgumentException("Invalid benchmark setting " + name);
    }
    return value;
  }
}
