package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.LockModeType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    change("a1", row -> row.failed(Long.MAX_VALUE, "delayed"));
    try (var actual = new JetStreamPublisher(config)) {
      var relay = new OutboxRelay(PerUserRelayIT::transaction, actual, config);
      assertEquals(1, relay.runBatch().published());
      assertEquals(OutboxRelay.Outcome.NO_WORK, relay.runBatch().outcome());
      assertFalse(row("a2").publicationMayHaveOccurred());
      change("a1", row -> row.failed(0, "due"));
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
                  change("a1", row -> row.failed(0, "new owner"));
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
        change("a1", row -> row.failed(Long.MAX_VALUE, "new owner"));
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
}
