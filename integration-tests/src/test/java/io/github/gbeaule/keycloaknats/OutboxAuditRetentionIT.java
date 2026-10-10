package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.gbeaule.keycloaknats.consumer.ConsumerConfig;
import io.github.gbeaule.keycloaknats.consumer.OutboxReport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.postgresql.ds.PGSimpleDataSource;

/** Real PostgreSQL retention, concurrent claims, rollback and source-state isolation. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OutboxAuditRetentionIT extends RelayIntegrationSupport {
  private static final class TestRegistry extends SimpleMeterRegistry implements AutoCloseable {}

  private static final String TABLE = "\"relay-data\".kc_nats_discard_audit";

  @Test
  void discardedElevenLeavesTenAndTwelveWithoutAReplacementOrReplayAfterCleanup() throws Exception {
    var event = new Event();
    event.setRealmId("gap");
    event.setUserId(UUID.randomUUID().toString());
    event.setType(EventType.LOGIN);
    var envelope = new EventEnvelope(config);
    var originals = new ArrayList<OutboxEvent>();
    transaction(
        em -> {
          long now = CaptureRepository.readDatabaseTime(em);
          for (int sequence = 1; sequence <= 12; sequence++) {
            var policy = sequence == 11 ? new PublicationPolicy(1, null) : PublicationPolicy.RETRY;
            var row =
                envelope.serialize(
                    UUID.randomUUID().toString(),
                    envelope.describe(event),
                    CaptureRepository.allocateNextSequence(
                        em, event.getRealmId(), event.getUserId()),
                    sequence == 11 ? now - 2000 : now,
                    new ResolvedPublicationPolicy(policy, EventFilter.all().sha256(), null));
            em.persist(row);
            originals.add(row);
          }
          OutboxHeads.refresh(em, originals.getFirst().orderingKey());
        });
    try (var publisher = new JetStreamPublisher(config);
        var metrics = new TestRegistry();
        var cleanup = cleanup(Map.of("audit-retention-seconds", "0"), metrics)) {
      var relay = new OutboxRelay(OutboxAuditRetentionIT::transaction, publisher, config);
      var result = relay.runBatch();
      assertEquals(11, result.published());
      assertEquals(1, result.expired());
      var published = storedMessages(11);
      assertEquals(
          "10",
          objectMapper.readTree(published.get(9).getData()).at("/data/ordering/sequence").asText());
      assertEquals(
          "12",
          objectMapper
              .readTree(published.get(10).getData())
              .at("/data/ordering/sequence")
              .asText());
      assertEquals(
          originals.stream().filter(o -> o.userSequence() != 11).map(OutboxEvent::id).toList(),
          storedIds(11));
      assertEquals(List.of(originals.get(10).id()), ids());
      cleanup.run();
      assertTrue(
          OutboxReport.inspect(database, "relay-data", 10, Long.MIN_VALUE, "", 10).isEmpty());
      transaction(
          em -> {
            var ordering =
                CaptureRepository.allocateNextSequence(em, event.getRealmId(), event.getUserId());
            assertEquals(13, ordering.sequence());
            em.persist(
                envelope.serialize(
                    UUID.randomUUID().toString(),
                    envelope.describe(event),
                    ordering,
                    CaptureRepository.readDatabaseTime(em),
                    new ResolvedPublicationPolicy(
                        PublicationPolicy.RETRY, EventFilter.all().sha256(), null)));
            OutboxHeads.refresh(em, ordering.key());
          });
      assertEquals(1, relay.runBatch().published());
      assertEquals(OutboxRelay.Outcome.NO_WORK, relay.runBatch().outcome());
      assertEquals(12, messages());
      assertEquals(0, audits());
      assertTrue(nats.jetStreamManagement().getConsumerNames(STREAM).isEmpty());
    }
  }

  @Test
  void exactCutoffIncludesEqualityAndUsesTheDiscardTimeIndex() throws Exception {
    transaction(
        em -> {
          long now = CaptureRepository.readDatabaseTime(em);
          long cutoff = now - Duration.ofDays(7).toMillis();
          audit(em, "before", cutoff - 1);
          audit(em, "equal", cutoff);
          audit(em, "after", cutoff + 1);
          em.flush();
          var claimed = AuditRepository.lockExpired(em, cutoff, 500);
          assertEquals(
              List.of("before", "equal"),
              claimed.stream()
                  .map(a -> em.getEntityManagerFactory().getPersistenceUnitUtil().getIdentifier(a))
                  .toList());
          claimed.forEach(em::remove);
        });
    assertEquals(List.of("after"), ids());
    assertEquals(
        1,
        scalar(
            "SELECT count(*) FROM pg_indexes WHERE schemaname='relay-data'"
                + " AND indexname='idx_kc_nats_discard_retention'"
                + " AND indexdef LIKE '%(discarded_at, id)%'"));
  }

  @Test
  void statisticsCountAllRetainedRowsButAgeOnlyEligibleDiscards() {
    transaction(
        em -> {
          long now = CaptureRepository.readDatabaseTime(em);
          final var retention = Duration.ofDays(7);
          audit(em, "recent", now - Duration.ofDays(1).toMillis());
          audit(em, "future", now + Duration.ofDays(1).toMillis());
          em.flush();
          var statistics = AuditRepository.statistics(em, retention);
          assertEquals(2, statistics.retainedRows());
          assertEquals(0, statistics.oldestEligibleAgeSeconds());

          long oldest = now - Duration.ofDays(9).toMillis();
          audit(em, "expired", now - Duration.ofDays(8).toMillis());
          audit(em, "oldest", oldest);
          em.flush();
          statistics = AuditRepository.statistics(em, retention);
          assertEquals(4, statistics.retainedRows());
          assertEquals(
              (statistics.databaseTime() - oldest) / 1000.0, statistics.oldestEligibleAgeSeconds());
        });
  }

  @Test
  void retentionChangesApplyToExistingHistoryAndZeroLeavesUncommittedAuditsAlone() {
    transaction(
        em -> {
          long now = CaptureRepository.readDatabaseTime(em);
          audit(em, "old", now - Duration.ofDays(8).toMillis());
          audit(em, "recent", now - Duration.ofDays(1).toMillis());
        });
    try (var metrics = new TestRegistry();
        var cleanup = cleanup(Map.of(), metrics)) {
      cleanup.run();
      assertEquals(List.of("recent"), ids());
      assertEquals(1, metrics.get("knd.audit.cleanup.deleted").counter().count());
    }
    try (var owner = sessions.openSession();
        var metrics = new TestRegistry();
        var cleanup = cleanup(Map.of("audit-retention-seconds", "0"), metrics)) {
      owner.beginTransaction();
      audit(owner, "uncommitted", CaptureRepository.readDatabaseTime(owner));
      owner.flush();
      cleanup.run();
      assertEquals(List.of(), ids());
      owner.getTransaction().commit();
      cleanup.run();
      assertEquals(List.of(), ids());
      assertEquals(2, metrics.get("knd.audit.cleanup.deleted").counter().count());
      assertEquals(0, metrics.get("knd.audit.retained.rows").gauge().value());
    }
  }

  @Test
  void scheduledCleanupDrainsWithoutNatsAndPreservesPayloadsAndSequenceIdentity() throws Exception {
    capture("protected", "retention-user");
    var original = row("protected");
    seed(1201);
    var docker = broker.getDockerClient();
    docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    try (var metrics = new TestRegistry();
        var cleanup = cleanup(Map.of("audit-cleanup-interval-ms", "1000"), metrics)) {
      cleanup.start();
      await().atMost(Duration.ofSeconds(20)).until(() -> audits() == 0);
      assertEquals(original.payload(), row("protected").payload());
      assertEquals(original.userSequence(), row("protected").userSequence());
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                assertEquals(1201, metrics.get("knd.audit.cleanup.deleted").counter().count());
                assertEquals(0, metrics.get("knd.audit.retained.rows").gauge().value());
              });
    } finally {
      docker.startContainerCmd(broker.getContainerId()).exec();
      connectNats();
    }
    assertEquals(0, messages());
    try (var publisher =
        new JetStreamPublisher(
            BridgeConfig.from(Map.of("nats-url", natsUrl(), "min-replicas", "1")))) {
      var relay = new OutboxRelay(OutboxAuditRetentionIT::transaction, publisher, config);
      assertEquals(1, relay.runBatch().published());
      capture("next", "retention-user");
      assertEquals(original.userSequence() + 1, row("next").userSequence());
      assertEquals(1, relay.runBatch().published());
      assertEquals(0, relay.runBatch().published());
      assertEquals(List.of("protected", "next"), storedIds(2));
    }
  }

  @Test
  void competingWorkersSkipOwnedRowsAndRestartRediscoversThem() throws Exception {
    seed(10);
    try (var owner = sessions.openSession();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var firstMetrics = new TestRegistry();
        var secondMetrics = new TestRegistry();
        var first =
            cleanup(
                Map.of("audit-cleanup-batch-size", "2", "audit-cleanup-max-batches", "1"),
                firstMetrics);
        var second =
            cleanup(
                Map.of("audit-cleanup-batch-size", "2", "audit-cleanup-max-batches", "1"),
                secondMetrics)) {
      owner.beginTransaction();
      owner.find(DiscardAudit.class, "audit-0", LockModeType.PESSIMISTIC_WRITE);
      var ready = new CountDownLatch(1);
      var running =
          executor.submit(
              () -> {
                ready.await();
                first.run();
                return null;
              });
      var competing =
          executor.submit(
              () -> {
                ready.await();
                second.run();
                return null;
              });
      ready.countDown();
      running.get(10, TimeUnit.SECONDS);
      competing.get(10, TimeUnit.SECONDS);
      assertEquals(6, audits());
      assertTrue(ids().contains("audit-0"));
      assertEquals(2, firstMetrics.get("knd.audit.cleanup.deleted").counter().count());
      assertEquals(2, secondMetrics.get("knd.audit.cleanup.deleted").counter().count());
      owner.getTransaction().rollback();
    }
    try (var metrics = new TestRegistry();
        var restarted = cleanup(Map.of(), metrics)) {
      restarted.run();
      assertEquals(0, audits());
      assertEquals(6, metrics.get("knd.audit.cleanup.deleted").counter().count());
      restarted.run();
      assertEquals(6, metrics.get("knd.audit.cleanup.deleted").counter().count());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"commit", "rollback", "interrupt", "commit-unknown"})
  void failedCleanupNeverLosesPendingWorkOrReportsAnUnconfirmedDeletion(String failure)
      throws Exception {
    seed(1);
    capture("pending", "failure-" + failure);
    if (failure.equals("commit")) {
      execute(
          "CREATE FUNCTION \"relay-data\".reject_cleanup() RETURNS trigger LANGUAGE plpgsql"
              + " AS $$ BEGIN RAISE EXCEPTION 'cleanup commit rejected'; END $$");
      execute(
          "CREATE CONSTRAINT TRIGGER reject_cleanup AFTER DELETE ON "
              + TABLE
              + " DEFERRABLE INITIALLY DEFERRED FOR EACH ROW"
              + " EXECUTE FUNCTION \"relay-data\".reject_cleanup()");
    }
    try (var metrics = new TestRegistry();
        var cleanup =
            new AuditCleanup(
                work -> {
                  transaction(
                      em -> {
                        work.accept(em);
                        if (failure.equals("rollback") || failure.equals("interrupt")) {
                          if (failure.equals("interrupt")) {
                            Thread.currentThread().interrupt();
                          }
                          throw new IllegalStateException("interrupted transaction");
                        }
                      });
                  if (failure.equals("commit-unknown")) {
                    throw new IllegalStateException("lost commit acknowledgement");
                  }
                },
                config.auditCleanup(),
                metrics)) {
      cleanup.run();
      Thread.interrupted();
      assertEquals(failure.equals("commit-unknown") ? 0 : 1, audits());
      assertEquals(0, metrics.get("knd.audit.cleanup.deleted").counter().count());
      assertEquals(1, metrics.get("knd.audit.cleanup.failures").counter().count());
      assertNotNull(row("pending"));
    } finally {
      Thread.interrupted();
      if (failure.equals("commit")) {
        execute("DROP TRIGGER reject_cleanup ON " + TABLE);
        execute("DROP FUNCTION \"relay-data\".reject_cleanup()");
      }
    }
    try (var metrics = new TestRegistry();
        var retry = cleanup(Map.of(), metrics)) {
      retry.run();
      assertEquals(0, audits());
      assertNotNull(row("pending"));
      assertEquals(0, messages());
    }
  }

  @Test
  void customSchemaInspectionAndCleanupNeedOnlyRuntimeGrants() throws Exception {
    seed(3);
    execute("CREATE ROLE audit_runtime LOGIN PASSWORD 'integration-only'");
    execute("GRANT USAGE ON SCHEMA \"relay-data\" TO audit_runtime");
    execute("GRANT SELECT, UPDATE, DELETE ON " + TABLE + " TO audit_runtime");
    var auditDatabase = new PGSimpleDataSource();
    auditDatabase.setUrl(postgres.getJdbcUrl());
    auditDatabase.setUser("audit_runtime");
    auditDatabase.setPassword("integration-only");
    try (var restricted =
            new Configuration()
                .addAnnotatedClass(DiscardAudit.class)
                .setProperty("hibernate.connection.url", postgres.getJdbcUrl())
                .setProperty("hibernate.connection.username", "audit_runtime")
                .setProperty("hibernate.connection.password", "integration-only")
                .setProperty("hibernate.default_schema", "\"relay-data\"")
                .setProperty("hibernate.hbm2ddl.auto", "validate")
                .buildSessionFactory();
        var metrics = new TestRegistry();
        var cleanup =
            new AuditCleanup(
                work -> restricted.inTransaction(work::accept), config.auditCleanup(), metrics)) {
      var page = OutboxReport.inspect(auditDatabase, "relay-data", 10, Long.MIN_VALUE, "", 2);
      assertEquals(2, page.size());
      var last = page.getLast();
      assertEquals(
          1,
          OutboxReport.inspect(
                  auditDatabase,
                  "relay-data",
                  10,
                  ((Number) last.get("discarded_at")).longValue(),
                  (String) last.get("id"),
                  2)
              .size());
      restricted.inTransaction(
          em -> {
            em.createNativeQuery("SET TRANSACTION READ ONLY").executeUpdate();
            assertEquals(3, AuditRepository.statistics(em, Duration.ofDays(7)).retainedRows());
          });
      cleanup.run();
      assertEquals(0, audits());
      assertEquals(3, metrics.get("knd.audit.cleanup.deleted").counter().count());
    }
  }

  @Test
  void auditScopeFiltersOtherRealmsAndSubjectsBeforeKeysetPaginationAndCliOutput()
      throws Exception {
    String ownLogin = "keycloak.events.cmVhbG0vYQ.user.login";
    String otherLogin = "keycloak.events.cmVhbG0vYg.user.login";
    transaction(
        em -> {
          audit(em, "a-own-login", 1000, "realm/a", ownLogin);
          audit(em, "b-other-login", 1000, "realm/b", otherLogin);
          audit(em, "c-own-logout", 1000, "realm/a", "keycloak.events.cmVhbG0vYQ.user.logout");
          audit(em, "d-own-login", 1000, "realm/a", ownLogin);
          audit(em, "e-own-login", 2000, "realm/a", ownLogin);
        });
    execute("CREATE ROLE audit_scope_reader LOGIN PASSWORD 'integration-only'");
    execute("GRANT USAGE ON SCHEMA \"relay-data\" TO audit_scope_reader");
    execute("GRANT SELECT ON " + TABLE + " TO audit_scope_reader");
    var values =
        Map.of(
            "KND_OUTBOX_DB_URL", postgres.getJdbcUrl(),
            "KND_OUTBOX_DB_USER", "audit_scope_reader",
            "KND_OUTBOX_DB_PASSWORD", "integration-only",
            "KND_OUTBOX_DB_SCHEMA", "relay-data",
            "KND_REPORT_SUBJECT", "keycloak.events.*.user.login",
            "KND_REPORT_REALM_ID", "realm/a");
    var settings = new ConsumerConfig(values::get).report();
    var first =
        OutboxReport.inspect(
            settings.database(),
            settings.schema(),
            settings.subject(),
            settings.realmPrefix(),
            settings.timeoutSeconds(),
            Long.MIN_VALUE,
            "",
            1);
    assertEquals(List.of("a-own-login"), first.stream().map(row -> row.get("id")).toList());
    var last = first.getLast();
    var remaining =
        OutboxReport.inspect(
            settings.database(),
            settings.schema(),
            settings.subject(),
            settings.realmPrefix(),
            settings.timeoutSeconds(),
            ((Number) last.get("discarded_at")).longValue(),
            (String) last.get("id"),
            10);
    assertEquals(
        List.of("d-own-login", "e-own-login"),
        remaining.stream().map(row -> row.get("id")).toList());
    assertEquals(
        4,
        OutboxReport.inspect(
                settings.database(),
                "relay-data",
                settings.subject(),
                "",
                5,
                Long.MIN_VALUE,
                "",
                10)
            .size());
    assertEquals(
        5,
        OutboxReport.inspect(settings.database(), "relay-data", 5, Long.MIN_VALUE, "", 10).size());

    var page = auditCli(values, "audit");
    assertEquals(3, page.size());
    assertEquals("a-own-login", page.get(0).path("id").asText());
    assertEquals("d-own-login", page.get(1).path("id").asText());
    assertEquals("e-own-login", page.get(2).path("id").asText());
    var continued = auditCli(values, "audit", "1000", "a-own-login", "1");
    assertEquals(1, continued.size());
    assertEquals("d-own-login", continued.get(0).path("id").asText());
  }

  @Test
  void largeBacklogDrainsWhileCapturesPublicationsAndNewAuditsContinue() throws Exception {
    try (var publisher = new JetStreamPublisher(config);
        var metrics = new TestRegistry();
        var cleanup = cleanup(Map.of("audit-cleanup-interval-ms", "1000"), metrics)) {
      var relay = new OutboxRelay(OutboxAuditRetentionIT::transaction, publisher, config);
      final var baseline = measureTraffic(relay, 10);
      seed(10000);
      final long started = System.nanoTime();
      cleanup.start();
      final var loaded = measureTraffic(relay, 20);
      // Ten thousand individual deletes can exceed 30 seconds on a loaded Docker host.
      await()
          .atMost(Duration.ofMinutes(2))
          .untilAsserted(() -> assertEquals(0, audits(), "Audit backlog must drain"));
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertEquals(10030, metrics.get("knd.audit.cleanup.deleted").counter().count()));
      assertEquals(0, metrics.get("knd.audit.cleanup.failures").counter().count());
      double drainSeconds = (System.nanoTime() - started) / 1_000_000_000.0;
      System.out.printf(
          "Audit backlog: drain=%.3fs rows/s=%.1f;"
              + " baseline capture/publish p95=%.3f/%.3fms; loaded=%.3f/%.3fms%n",
          drainSeconds, 10020 / drainSeconds, baseline[0], baseline[1], loaded[0], loaded[1]);
      assertEquals(30, messages());
      assertEquals(0, scalar("SELECT count(*) FROM \"relay-data\".kc_nats_outbox"));
    }
  }

  private static double[] measureTraffic(OutboxRelay relay, int count) {
    var captureTimes = new ArrayList<Long>();
    var publishTimes = new ArrayList<Long>();
    for (int i = 0; i < count; i++) {
      String id = UUID.randomUUID().toString();
      long start = System.nanoTime();
      capture(id, id);
      captureTimes.add(System.nanoTime() - start);
      start = System.nanoTime();
      assertEquals(1, relay.runBatch().published());
      publishTimes.add(System.nanoTime() - start);
      transaction(em -> audit(em, id, 0));
    }
    captureTimes.sort(Long::compareTo);
    publishTimes.sort(Long::compareTo);
    int percentile = (int) Math.ceil(count * 0.95) - 1;
    return new double[] {
      captureTimes.get(percentile) / 1_000_000.0, publishTimes.get(percentile) / 1_000_000.0
    };
  }

  private static AuditCleanup cleanup(Map<String, String> settings, SimpleMeterRegistry registry) {
    return new AuditCleanup(
        OutboxAuditRetentionIT::transaction, BridgeConfig.from(settings).auditCleanup(), registry);
  }

  private static void seed(int count) {
    transaction(
        em -> {
          for (int i = 0; i < count; i++) {
            audit(em, "audit-" + i, 0);
            if (i % 500 == 499) {
              em.flush();
              em.clear();
            }
          }
        });
  }

  private static void audit(EntityManager em, String id, long discardedAt) {
    audit(em, id, discardedAt, "relay", "keycloak.events.audit");
  }

  private static void audit(
      EntityManager em, String id, long discardedAt, String realmId, String subject) {
    var original =
        new OutboxEvent(
            id,
            subject,
            "private payload",
            0,
            realmId,
            "io.keycloak.user.login",
            null,
            new ResolvedPublicationPolicy(
                new PublicationPolicy(1, null), EventFilter.digest(new byte[0]), "retention-test"));
    em.persist(new DiscardAudit(original, DiscardReason.EXPIRED, discardedAt));
  }

  private static JsonNode auditCli(Map<String, String> reportSettings, String... arguments)
      throws Exception {
    boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
    String executable =
        Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java").toString();
    String artifact = System.getProperty("consumer.jar");
    assertTrue(Files.isRegularFile(Path.of(artifact)), "Package the consumer before its CLI tests");
    var command =
        new ArrayList<>(List.of(executable, "-cp", artifact, OutboxReport.class.getName()));
    command.addAll(List.of(arguments));
    var builder = new ProcessBuilder(command).redirectErrorStream(true);
    var environment = builder.environment();
    environment.keySet().removeIf(key -> key.toUpperCase(Locale.ROOT).startsWith("KND_"));
    environment.putAll(reportSettings);
    environment.put("KND_CONSUMER_DEADLINE_MS", "invalid");
    environment.put("KND_NATS_URL", "invalid");
    environment.put("KND_TOKEN", "token");
    environment.put("KND_CREDENTIALS_FILE", "conflicting.creds");
    Process process = builder.start();
    try {
      assertTrue(
          process.waitFor(20, TimeUnit.SECONDS), "Audit inspection must finish within its bound");
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(0, process.exitValue(), output);
      return objectMapper.readTree(output);
    } finally {
      process.destroyForcibly();
      process.waitFor(5, TimeUnit.SECONDS);
    }
  }

  private static List<String> ids() {
    try (var session = sessions.openSession()) {
      return session
          .createQuery("select a.id from NatsDiscardAudit a order by a.id", String.class)
          .getResultList();
    }
  }

  private static long audits() throws Exception {
    return scalar("SELECT count(*) FROM " + TABLE);
  }
}
