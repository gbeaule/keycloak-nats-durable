package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.LockModeType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.hibernate.resource.jdbc.spi.PhysicalConnectionHandlingMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Exercises the production relay with PostgreSQL ownership and real JetStream acknowledgements. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class PerUserRelayIT extends RelayIntegrationSupport {
  @Test
  void lockedHeadAndUncommittedRemovalBlockSuccessorsAcrossSubjects() throws Exception {
    capture("a1", "a");
    capture("a2", "a");
    capture("b1", "b");
    capture("independent1", null);
    capture("independent2", null);
    transaction(em -> capture(em, "other-realm", "another-realm", "a"));
    try (var owner = sessions.openSession();
        var publisher = new JetStreamPublisher(config)) {
      owner.beginTransaction();
      var head = owner.find(OutboxEvent.class, "a1", LockModeType.PESSIMISTIC_WRITE);
      var relay = new OutboxRelay(PerUserRelayIT::transaction, publisher, config);
      assertEquals(4, relay.runBatch().published());
      assertFalse(row("a2").publicationMayHaveOccurred());
      owner.remove(head);
      owner.flush();
      assertEquals(OutboxRelay.Outcome.NO_WORK, relay.runBatch().outcome());
      owner.getTransaction().rollback();
      assertEquals(2, relay.runBatch().published());
    }
    var ids = storedIds(6);
    assertEquals(
        Set.of("a1", "a2", "b1", "independent1", "independent2", "other-realm"), Set.copyOf(ids));
    assertTrue(ids.indexOf("a1") < ids.indexOf("a2"));
  }

  @Test
  void delayedOrFailingHeadsDoNotStarveOtherUsersOrIndependentEvents() throws Exception {
    capture("a1", "a");
    capture("a2", "a");
    capture("b1", "b");
    change("a1", row -> row.recordPublicationFailure(Long.MAX_VALUE, "delayed"));
    try (var actual = new JetStreamPublisher(config)) {
      var relay = new OutboxRelay(PerUserRelayIT::transaction, actual, config);
      assertEquals(1, relay.runBatch().published());
      assertEquals(OutboxRelay.Outcome.NO_WORK, relay.runBatch().outcome());
      assertFalse(row("a2").publicationMayHaveOccurred());
      change("a1", row -> row.recordPublicationFailure(0, "due"));
      capture("b2", "b");
      var failures = new AtomicInteger();
      var failing =
          publisher(
              event -> {
                if (event.id().equals("a1")) {
                  failures.incrementAndGet();
                  throw new IOException("injected failure");
                }
                actual.publish(event);
              });
      var result = new OutboxRelay(PerUserRelayIT::transaction, failing, config).runBatch();
      assertEquals(new OutboxRelay.BatchResult(2, 1, 1, 0, 0, OutboxRelay.Outcome.NO_WORK), result);
      assertEquals(1, failures.get());
      assertFalse(row("a2").publicationMayHaveOccurred());
    }
    assertEquals(List.of("b1", "b2"), storedIds(2));
  }

  @Test
  void activePublicationBlocksOnlyItsUserAndNeverTheCaptureCounter() throws Exception {
    capture("a1", "a");
    capture("a2", "a");
    var sent = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var first = new JetStreamPublisher(config);
        var second = new JetStreamPublisher(config)) {
      var blocked =
          publisher(
              event -> {
                first.publish(event);
                if (event.id().equals("a1")) {
                  sent.countDown();
                  assertTrue(release.await(15, TimeUnit.SECONDS));
                }
              });
      var running =
          executor.submit(new OutboxRelay(PerUserRelayIT::transaction, blocked, config)::runBatch);
      try {
        assertTrue(sent.await(10, TimeUnit.SECONDS));
        capture("b1", "b");
        assertEquals(
            1, new OutboxRelay(PerUserRelayIT::transaction, second, config).runBatch().published());
        transaction(
            em -> {
              em.createNativeQuery("SET LOCAL lock_timeout = '1s'").executeUpdate();
              capture(em, "a3", "relay", "a");
            });
        assertFalse(row("a2").publicationMayHaveOccurred());
        assertFalse(row("a3").publicationMayHaveOccurred());
      } finally {
        release.countDown();
      }
      assertEquals(3, running.get(10, TimeUnit.SECONDS).published());
    }
    assertEquals(List.of("a1", "b1", "a2", "a3"), storedIds(4));
  }

  @Test
  void uncommittedCaptureAndRollbackNeverExposeLaterPositions() throws Exception {
    try (var capture = sessions.openSession();
        var publisher = new JetStreamPublisher(config)) {
      capture.beginTransaction();
      capture(capture, "rolled-back1", "rollback", "a");
      capture(capture, "rolled-back2", "rollback", "a");
      capture.flush();
      var relay = new OutboxRelay(PerUserRelayIT::transaction, publisher, config);
      assertEquals(OutboxRelay.Outcome.NO_WORK, relay.runBatch().outcome());
      capture.getTransaction().rollback();
      transaction(em -> capture(em, "committed", "rollback", "a"));
      assertEquals(1L, row("committed").userSequence());
      assertEquals(1, relay.runBatch().published());
    }
    assertEquals(List.of("committed"), storedIds(1));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rollbackAfterSendKeepsIntentAndOriginalRetryBytes(boolean loseAck) throws Exception {
    capture("a1", "a");
    capture("a2", "a");
    var original = row("a1");
    var transactions = new AtomicInteger();
    try (var actual = new JetStreamPublisher(config)) {
      var network =
          publisher(
              event -> {
                assertTrue(
                    row(event.id()).publicationMayHaveOccurred(),
                    "Intent must already be committed");
                actual.publish(event);
                if (loseAck) {
                  throw new IOException("acknowledgement lost after acceptance");
                }
              });
      var relay =
          new OutboxRelay(
              work ->
                  transaction(
                      em -> {
                        work.accept(em);
                        em.flush();
                        if (transactions.incrementAndGet() == 2) {
                          throw new IllegalStateException("rollback after send");
                        }
                      }),
              network,
              config);
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, relay.runBatch().outcome());
      var retained = row("a1");
      assertTrue(retained.publicationMayHaveOccurred());
      assertEquals(0, retained.attempts(), "A rolled-back count is not proof of no send");
      assertEquals(original.payload(), retained.payload());
      assertEquals(original.subject(), retained.subject());
      assertFalse(row("a2").publicationMayHaveOccurred());
      assertEquals(1, messages());
      assertTrue(retained.nextAttemptAt() > System.currentTimeMillis());
      assertEquals(
          OutboxRelay.Outcome.NO_WORK,
          new OutboxRelay(PerUserRelayIT::transaction, actual, config).runBatch().outcome());
      change("a1", row -> row.deferResolution(0));
      assertEquals(
          2, new OutboxRelay(PerUserRelayIT::transaction, actual, config).runBatch().published());
      assertEquals(2, messages(), "Retries use the original NATS message ID for deduplication");
    }
    var delivered = storedMessages(2);
    assertEquals(original.subject(), delivered.getFirst().getSubject());
    assertEquals(
        original.payload(), new String(delivered.getFirst().getData(), StandardCharsets.UTF_8));
    assertEquals("a2", objectMapper.readTree(delivered.get(1).getData()).path("id").asText());
  }

  @Test
  void changedVersionBetweenIntentAndOwnershipCannotBePublished() throws Exception {
    capture("a1", "a");
    var transactions = new AtomicInteger();
    try (var actual = new JetStreamPublisher(config)) {
      var relay =
          new OutboxRelay(
              work -> {
                transaction(work);
                if (transactions.incrementAndGet() == 1) {
                  change("a1", row -> row.recordPublicationFailure(0, "new owner"));
                }
              },
              actual,
              BridgeConfig.from(
                  Map.of("nats-url", natsUrl(), "min-replicas", "1", "batch-size", "1")));
      var result = relay.runBatch();
      assertEquals(OutboxRelay.Outcome.STALE, result.outcome());
      assertEquals(1, result.processed());
      assertEquals(0, result.published());
      assertEquals(1, row("a1").attempts());
      assertEquals(0, messages());
    }
  }

  @Test
  void lostDatabaseOwnershipCannotDeleteNewerStateAfterAnInflightPublish() throws Exception {
    capture("a1", "a");
    capture("a2", "a");
    var backend = new AtomicLong();
    var sent = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var actual = new JetStreamPublisher(config)) {
      var network =
          publisher(
              event -> {
                actual.publish(event);
                sent.countDown();
                assertTrue(release.await(15, TimeUnit.SECONDS));
              });
      var relay =
          new OutboxRelay(
              work ->
                  transaction(
                      em -> {
                        backend.set(
                            ((Number)
                                    em.createNativeQuery("select pg_backend_pid()")
                                        .getSingleResult())
                                .longValue());
                        work.accept(em);
                      }),
              network,
              config);
      var running = executor.submit(relay::runBatch);
      try {
        assertTrue(sent.await(10, TimeUnit.SECONDS));
        transaction(
            em ->
                assertEquals(
                    Boolean.TRUE,
                    em.createNativeQuery("select pg_terminate_backend(:pid)", Boolean.class)
                        .setParameter("pid", Math.toIntExact(backend.get()))
                        .getSingleResult()));
        change("a1", row -> row.recordPublicationFailure(Long.MAX_VALUE, "new owner"));
      } finally {
        release.countDown();
      }
      assertEquals(
          OutboxRelay.Outcome.TRANSACTION_FAILED, running.get(10, TimeUnit.SECONDS).outcome());
      assertEquals(1, row("a1").attempts());
      assertTrue(row("a1").publicationMayHaveOccurred());
      assertFalse(row("a2").publicationMayHaveOccurred());
      assertEquals(1, messages(), "Losing a database lock cannot retract already-sent bytes");
    }
  }

  @Test
  void headLookupIndexesExistInTheCustomSchema() throws Exception {
    assertEquals(
        3,
        scalar(
            """
            SELECT count(*) FROM pg_indexes WHERE schemaname='relay-data'
              AND tablename='kc_nats_outbox' AND (
                indexdef LIKE '%(ordering_key, user_sequence)%' OR
                indexdef LIKE '%(next_attempt_at, created_at, id)%')
            """));
    assertEquals(
        1,
        scalar(
            "SELECT count(*) FROM pg_indexes WHERE schemaname='relay-data'"
                + " AND indexname='idx_kc_nats_head_due'"));
    assertEquals(
        0,
        scalar(
            "SELECT count(*) FROM pg_tables WHERE schemaname='public'"
                + " AND tablename='kc_nats_outbox'"));
  }

  @Test
  void boundedCounterLockWaitRetainsTheOriginalAndItsUserHead() throws Exception {
    capture("locked-counter-head", "bounded-lock");
    capture("locked-counter-successor", "bounded-lock");
    var original = row("locked-counter-head");
    var successor = row("locked-counter-successor");
    var bounded =
        BridgeConfig.from(
            Map.of(
                "batch-size", "1",
                "retry-initial-ms", "1",
                "retry-max-ms", "1",
                "relay-statement-timeout-ms", "250",
                "relay-lock-timeout-ms", "100"));
    var phases = new AtomicInteger();
    var sends = new ArrayList<String>();
    var failures = new ArrayList<String>();
    String projectionSql =
        """
        select last_sequence, head_event_id, head_next_attempt_at, head_created_at
          from "relay-data".kc_nats_capture_counter where ordering_key=:key
        """;
    try (var holder = sessions.openSession();
        var observer = sessions.openSession()) {
      var originalHead =
          observer
              .createNativeQuery(projectionSql, Object[].class)
              .setParameter("key", original.orderingKey())
              .getSingleResult();
      var relay =
          new OutboxRelay(
              work -> {
                try {
                  transaction(work);
                } catch (RuntimeException failure) {
                  Throwable cause = failure;
                  while (!(cause instanceof SQLException) && cause.getCause() != null) {
                    cause = cause.getCause();
                  }
                  failures.add(cause instanceof SQLException sql ? sql.getSQLState() : null);
                  throw failure;
                }
                if (phases.incrementAndGet() == 1) {
                  // Acquire the counter after intent commits. Ownership reacquisition and send do
                  // not need it; resolving the locked original must wait only for the local bound.
                  holder.beginTransaction();
                  holder.find(
                      CaptureCounter.class, original.orderingKey(), LockModeType.PESSIMISTIC_WRITE);
                }
              },
              publisher(event -> sends.add(event.id())),
              bounded);
      try {
        long started = System.nanoTime();
        assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, relay.runBatch().outcome());
        assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
        assertEquals(
            List.of("55P03", "55P03"),
            failures,
            "Both resolution and its deferral hit lock_timeout");
        var retained = row(original.id());
        assertTrue(retained.publicationMayHaveOccurred());
        assertEquals(0, retained.attempts());
        assertEquals(original.payload(), retained.payload());
        assertEquals(original.subject(), retained.subject());
        assertEquals(original.userSequence(), retained.userSequence());
        assertEquals(original.nextAttemptAt(), retained.nextAttemptAt());
        assertEquals(successor.userSequence(), row(successor.id()).userSequence());
        assertFalse(row(successor.id()).publicationMayHaveOccurred());
        assertArrayEquals(
            originalHead,
            observer
                .createNativeQuery(projectionSql, Object[].class)
                .setParameter("key", original.orderingKey())
                .getSingleResult(),
            "Neither failed resolution can advance the head or durable sequence");
      } finally {
        if (holder.getTransaction().isActive()) {
          holder.getTransaction().rollback();
        }
      }
      assertEquals(1, relay.runBatch().published());
      assertEquals(1, relay.runBatch().published());
      assertEquals(
          List.of(original.id(), original.id(), successor.id()),
          sends,
          "An unconfirmed resolution retries the original before its successor");
      assertArrayEquals(
          new Object[] {originalHead[0], null, null, null},
          observer
              .createNativeQuery(projectionSql, Object[].class)
              .setParameter("key", original.orderingKey())
              .getSingleResult());
    }
  }

  @Test
  void statementDeadlineAndTransactionLocalSettingsDoNotChangeThePooledConnection()
      throws Exception {
    String settingsSql =
        "select current_setting('statement_timeout'), current_setting('lock_timeout'),"
            + " pg_backend_pid()";
    // Retain the borrowed physical connection so restoration cannot pass by switching backends.
    try (var em =
        sessions
            .withOptions()
            .connectionHandlingMode(PhysicalConnectionHandlingMode.DELAYED_ACQUISITION_AND_HOLD)
            .openSession()) {
      em.beginTransaction();
      final var original = em.createNativeQuery(settingsSql, Object[].class).getSingleResult();
      capture(em, "timed-out-capture", "bounded-statements", "same-connection");
      RelayDatabase.limitStatements(em, new RelayDatabaseConfig(10, 100, 50));
      var limited = em.createNativeQuery(settingsSql, Object[].class).getSingleResult();
      assertEquals("100ms", limited[0]);
      assertEquals("50ms", limited[1]);
      assertEquals(original[2], limited[2]);
      long started = System.nanoTime();
      var failure =
          assertThrows(
              RuntimeException.class,
              () -> em.createNativeQuery("select 1 from pg_sleep(5)").getSingleResult());
      Throwable cause = failure;
      while (!(cause instanceof SQLException) && cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertEquals("57014", assertInstanceOf(SQLException.class, cause).getSQLState());
      assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
      em.getTransaction().rollback();
      em.clear();
      assertNull(row("timed-out-capture"), "Canceled SQL must roll back earlier outbox mutations");
      em.beginTransaction();
      assertArrayEquals(
          original,
          em.createNativeQuery(settingsSql, Object[].class).getSingleResult(),
          "Rollback restores both settings on the same backend");
      RelayDatabase.limitStatements(em, new RelayDatabaseConfig(10, 100, 50));
      assertArrayEquals(
          limited, em.createNativeQuery(settingsSql, Object[].class).getSingleResult());
      em.getTransaction().commit();

      em.beginTransaction();
      assertArrayEquals(
          original,
          em.createNativeQuery(settingsSql, Object[].class).getSingleResult(),
          "Commit also restores both settings on the same backend");
      capture(em, "capture-after-relay", "bounded-statements", "same-connection");
      assertEquals(
          1,
          ((Number) em.createNativeQuery("select 1 from pg_sleep(0.2)").getSingleResult())
              .intValue(),
          "A later capture transaction must not inherit the relay's 100ms statement deadline");
      em.getTransaction().commit();
    }
    assertEquals(1L, row("capture-after-relay").userSequence());
  }
}
