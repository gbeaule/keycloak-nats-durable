package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import org.hibernate.Timeouts;
import org.hibernate.jpa.SpecHints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxRelayTest {
  private EntityManager em;
  private TypedQuery<OutboxEvent> query;
  private TypedQuery<OutboxEvent> heads;
  private TypedQuery<OutboxEvent> prepared;
  private TypedQuery<OutboxEvent> expired;
  private TypedQuery<OutboxEvent> unchanged;
  private Query clock;
  private EventPublisher publisher;
  private OutboxEvent row;
  private OutboxRelay relay;
  private SimpleMeterRegistry registry;
  private RelayMetrics metrics;
  private org.mockito.MockedStatic<OutboxHeads> headUpdates;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setup() {
    em = mock(EntityManager.class);
    headUpdates = org.mockito.Mockito.mockStatic(OutboxHeads.class);
    publisher = mock(EventPublisher.class);
    query = mock(TypedQuery.class, RETURNS_SELF);
    heads = mock(TypedQuery.class, RETURNS_SELF);
    prepared = mock(TypedQuery.class, RETURNS_SELF);
    expired = mock(TypedQuery.class, RETURNS_SELF);
    unchanged = mock(TypedQuery.class, RETURNS_SELF);
    clock = mock(Query.class);
    when(em.createNativeQuery(anyString(), eq(Long.class))).thenReturn(clock);
    when(clock.getSingleResult()).thenAnswer(call -> System.currentTimeMillis());
    when(em.createQuery(anyString(), eq(OutboxEvent.class)))
        .thenAnswer(
            call -> {
              String hql = call.getArgument(0);
              if (hql.contains("NatsCaptureCounter")) {
                return heads;
              }
              return hql.contains(":version")
                  ? (hql.contains("event.nextAttemptAt") ? prepared : unchanged)
                  : hql.contains("event.nextExpiryAttemptAt") ? expired : query;
            });
    row = CaptureFixtures.row("id", "subject", "{}", 0);
    when(query.getResultList()).thenReturn(List.of(row), List.of());
    when(prepared.getResultList()).thenReturn(List.of(row));
    when(unchanged.getResultList()).thenAnswer(call -> List.of(row));
    registry = new SimpleMeterRegistry();
    metrics = new RelayMetrics(registry);
    relay =
        new OutboxRelay(work -> work.accept(em), publisher, BridgeConfig.from(Map.of()), metrics);
  }

  @AfterEach
  void closeMetrics() {
    metrics.close();
    assertTrue(registry.getMeters().isEmpty());
    registry.close();
    headUpdates.close();
  }

  @Test
  void removesOnlyAfterSuccessfulAcknowledgement() throws Exception {
    assertEquals(1, relay.runBatch().published());
    assertEquals(1, registry.get("knd.publication.confirmed").counter().count());
    var order = inOrder(publisher, em);
    order.verify(publisher).publish(row);
    order.verify(em).remove(row);
    verify(query, times(2)).setMaxResults(1);
    verify(query, times(2)).setLockMode(LockModeType.PESSIMISTIC_WRITE);
    verify(prepared).setLockMode(LockModeType.PESSIMISTIC_WRITE);
    verify(prepared).setParameter("id", row.id());
    verify(prepared).setParameter("version", row.version());
    verify(prepared).setHint(SpecHints.HINT_SPEC_LOCK_TIMEOUT, Timeouts.SKIP_LOCKED_MILLI);
    verify(query, times(2)).setHint(SpecHints.HINT_SPEC_LOCK_TIMEOUT, Timeouts.SKIP_LOCKED_MILLI);
  }

  @Test
  void lostPublishAckLeavesOriginalIdAndPayloadForRetry() throws Exception {
    doThrow(new IOException("ack lost")).when(publisher).publish(row);
    final long before = System.currentTimeMillis();
    assertEquals(0, relay.runBatch().published());
    assertEquals(1, registry.get("knd.publication.failures").counter().count());
    assertEquals(1, registry.get("knd.publication.retries").counter().count());
    final long after = System.currentTimeMillis();
    verify(em, never()).remove(any());
    assertEquals("id", row.id());
    assertEquals("{}", row.payload());
    assertEquals("subject", row.subject());
    assertEquals(0, row.createdAt());
    assertEquals(1, row.attempts());
    assertTrue(row.nextAttemptAt() >= before + 500);
    assertTrue(row.nextAttemptAt() <= after + 1000);
    assertEquals("IOException", row.lastError());
  }

  @Test
  void interruptedPublishRetainsRowAndInterruptFlag() throws Exception {
    doThrow(new InterruptedException()).when(publisher).publish(row);
    try {
      assertEquals(
          new OutboxRelay.BatchResult(1, 0, 0, 0, 0, OutboxRelay.Outcome.STOPPED),
          relay.runBatch());
      assertEquals(0, registry.get("knd.publication.transaction.failures").counter().count());
      assertTrue(Thread.currentThread().isInterrupted());
      verify(em, never()).remove(any());
      assertEquals(0, row.attempts());
      assertEquals(0, row.nextAttemptAt());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void shutdownDuringNetworkFailureDoesNotIncrementAttempts() throws Exception {
    org.mockito.Mockito.doAnswer(
            call -> {
              relay.stop();
              throw new IOException("connection closed during shutdown");
            })
        .when(publisher)
        .publish(row);
    assertEquals(OutboxRelay.Outcome.STOPPED, relay.runBatch().outcome());
    assertEquals(0, row.attempts());
    assertTrue(row.publicationMayHaveOccurred());
    verify(em, never()).remove(any());
  }

  @Test
  void sustainedFailuresLogAtPowersOfTwoWithoutExposingRemoteText() throws Exception {
    when(query.getResultList()).thenReturn(List.of(row));
    relay =
        new OutboxRelay(
            work -> work.accept(em), publisher, BridgeConfig.from(Map.of("batch-size", "1")));
    doThrow(new IOException("private-server-message")).when(publisher).publish(row);
    try (var logs = new LogCapture(OutboxRelay.class)) {
      for (int attempt = 1; attempt <= 9; attempt++) {
        assertEquals(0, relay.runBatch().published());
      }
      var records = logs.records();
      assertEquals(4, records.size());
      var formatter = new SimpleFormatter();
      for (int i = 0; i < records.size(); i++) {
        var record = records.get(i);
        assertEquals(Level.WARNING, record.getLevel());
        assertTrue(formatter.format(record).contains(" attempts=" + (1L << i) + " retryAt="));
        assertFalse(formatter.format(record).contains("private-server-message"));
        assertNull(record.getThrown());
      }
    }
    assertEquals(9, row.attempts());
    verify(em, never()).remove(any());
  }

  @Test
  void failedCommitDoesNotCountAsDeliveryAndTheOriginalRowCanBeRepublished() throws Exception {
    var completed = new AtomicInteger();
    when(query.getResultList()).thenReturn(List.of(row), List.of(row), List.of());
    var worker =
        new OutboxRelay(
            work -> {
              work.accept(em);
              if (completed.getAndIncrement() == 1) {
                throw new IllegalStateException("commit failed after broker acknowledgement");
              }
            },
            publisher,
            BridgeConfig.from(Map.of()),
            metrics);
    assertEquals(0, worker.runBatch().published());
    assertEquals(0, registry.get("knd.publication.confirmed").counter().count());
    assertEquals(1, worker.runBatch().published());
    assertEquals(1, registry.get("knd.publication.confirmed").counter().count());
    verify(publisher, times(2)).publish(row);
    verify(em, times(2)).remove(row);
    assertEquals("id", row.id());
    assertEquals("subject", row.subject());
    assertEquals("{}", row.payload());
    assertEquals(0, row.attempts());
  }

  @Test
  void stopPreventsMoreWork() {
    relay.stop();
    relay.run();
    verifyNoInteractions(publisher, em, query);
  }

  @Test
  void stopWhileObtainingTheRowPreventsPublication() {
    when(query.getResultList())
        .thenAnswer(
            call -> {
              relay.stop();
              return List.of(row);
            });
    relay.run();
    verifyNoInteractions(publisher);
    verify(em, never()).remove(any());
    assertEquals(0, row.attempts());
  }

  @Test
  void interruptWhileObtainingTheRowPreventsPublication() {
    when(query.getResultList())
        .thenAnswer(
            call -> {
              Thread.currentThread().interrupt();
              return List.of(row);
            });
    try {
      relay.run();
      verifyNoInteractions(publisher);
      verify(em, never()).remove(any());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void anEmptyQueueDoesNotPublishOrDeleteAnything() {
    when(query.getResultList()).thenReturn(List.of());
    assertEquals(
        new OutboxRelay.BatchResult(0, 0, 0, 0, 0, OutboxRelay.Outcome.NO_WORK), relay.runBatch());
    assertEquals(0, registry.get("knd.publication.transaction.failures").counter().count());
    verifyNoInteractions(publisher);
    verify(em, never()).remove(any());
    verify(query).getResultList();
  }

  @Test
  void fullBatchStopsAtItsConfiguredSizeWithoutClaimingAnotherRow() throws Exception {
    var second = CaptureFixtures.row("second", "subject", "{\"n\":2}", 10);
    when(query.getResultList()).thenReturn(List.of(row), List.of(second));
    when(prepared.getResultList()).thenReturn(List.of(row), List.of(second));
    var worker =
        new OutboxRelay(
            work -> work.accept(em), publisher, BridgeConfig.from(Map.of("batch-size", "2")));
    assertEquals(2, worker.runBatch().published());
    verify(query, times(2)).getResultList();
    var order = inOrder(publisher, em);
    order.verify(publisher).publish(row);
    order.verify(em).remove(row);
    order.verify(publisher).publish(second);
    order.verify(em).remove(second);
  }

  @Test
  void interruptionBeforeStartingPreventsOpeningTransactions() {
    try {
      Thread.currentThread().interrupt();
      assertEquals(0, relay.runBatch().published());
      verifyNoInteractions(em, query, publisher);
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void repeatedPublicationFailuresKeepRetryingWithoutChangingOriginalEvent() throws Exception {
    when(query.getResultList()).thenReturn(List.of(row));
    relay =
        new OutboxRelay(
            work -> work.accept(em), publisher, BridgeConfig.from(Map.of("batch-size", "1")));
    doThrow(new IOException("private-server-message")).when(publisher).publish(row);
    for (int attempt = 1; attempt <= 4; attempt++) {
      assertEquals(0, relay.runBatch().published());
      assertEquals(attempt, row.attempts());
      assertEquals("IOException", row.lastError());
      assertEquals("id", row.id());
      assertEquals("subject", row.subject());
      assertEquals("{}", row.payload());
      assertEquals(0, row.createdAt());
    }
    verify(em, never()).remove(any());
  }

  @Test
  void publicationStartsOnlyAfterIntentCommits() throws Exception {
    var committed = new AtomicInteger();
    var worker =
        new OutboxRelay(
            work -> {
              work.accept(em);
              committed.incrementAndGet();
            },
            publisher,
            BridgeConfig.from(Map.of()));
    org.mockito.Mockito.doAnswer(
            call -> {
              assertEquals(1, committed.get());
              assertTrue(row.publicationMayHaveOccurred());
              return null;
            })
        .when(publisher)
        .publish(row);
    assertEquals(1, worker.runBatch().published());
    var order = inOrder(em, publisher);
    order.verify(em).flush();
    order.verify(publisher).publish(row);
    order.verify(em).remove(row);
  }

  @Test
  void failedIntentCommitNeverStartsPublication() {
    var worker =
        new OutboxRelay(
            work -> {
              work.accept(em);
              throw new IllegalStateException("intent commit failed");
            },
            publisher,
            BridgeConfig.from(Map.of()));
    assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, worker.runBatch().outcome());
    verifyNoInteractions(publisher);
    verify(em, never()).remove(any());
  }

  @Test
  void staleClaimsConsumeTheBatchWithoutPublishingOrSpinning() {
    when(query.getResultList()).thenReturn(List.of(row));
    when(prepared.getResultList()).thenReturn(List.of());
    var worker =
        new OutboxRelay(
            work -> work.accept(em), publisher, BridgeConfig.from(Map.of("batch-size", "2")));
    assertEquals(
        new OutboxRelay.BatchResult(2, 0, 0, 0, 0, OutboxRelay.Outcome.STALE), worker.runBatch());
    verify(query, times(2)).getResultList();
    verify(prepared, times(2)).getResultList();
    verifyNoInteractions(publisher);
    verify(em, never()).remove(any());
  }

  @Test
  void failedHeadLeavesRoomForOtherUsersInTheSameBatch() throws Exception {
    var second = CaptureFixtures.row("other", "other.subject", "{}", 10);
    when(query.getResultList()).thenReturn(List.of(row), List.of(second));
    when(prepared.getResultList()).thenReturn(List.of(row), List.of(second));
    doThrow(new IOException("unavailable")).when(publisher).publish(row);
    var worker =
        new OutboxRelay(
            work -> work.accept(em), publisher, BridgeConfig.from(Map.of("batch-size", "2")));
    assertEquals(
        new OutboxRelay.BatchResult(2, 1, 1, 0, 0, OutboxRelay.Outcome.PUBLISHED),
        worker.runBatch());
    verify(publisher).publish(second);
    verify(em).remove(second);
    verify(em, never()).remove(row);
  }

  @Test
  void stopAfterIntentCommitPreventsSending() {
    var workerRef = new java.util.concurrent.atomic.AtomicReference<OutboxRelay>();
    var worker =
        new OutboxRelay(
            work -> {
              work.accept(em);
              workerRef.get().stop();
            },
            publisher,
            BridgeConfig.from(Map.of()));
    workerRef.set(worker);
    assertEquals(OutboxRelay.Outcome.STOPPED, worker.runBatch().outcome());
    verifyNoInteractions(publisher, prepared);
  }

  @Test
  void stopWhileReacquiringOwnershipRetainsThePreparedRow() {
    when(prepared.getResultList())
        .thenAnswer(
            call -> {
              relay.stop();
              return List.of(row);
            });
    assertEquals(OutboxRelay.Outcome.STOPPED, relay.runBatch().outcome());
    assertTrue(row.publicationMayHaveOccurred());
    verifyNoInteractions(publisher);
    verify(em, never()).remove(any());
  }

  @Test
  void expiredRowsAreDiscardedWithoutIntentOrNetworkWork() {
    usePolicy(new PublicationPolicy(1, null));
    when(expired.getResultList()).thenReturn(List.of(row), List.of());
    when(query.getResultList()).thenReturn(List.of());
    var result = relay.runBatch();
    assertEquals(1, result.expired());
    assertEquals(1, result.discarded());
    assertFalse(row.publicationMayHaveOccurred());
    verify(em).persist(any(DiscardAudit.class));
    verify(em).remove(row);
    verifyNoInteractions(publisher, prepared);
    verify(expired, times(2)).setMaxResults(1);
    verify(expired, times(2)).setLockMode(LockModeType.PESSIMISTIC_WRITE);
    verify(expired, times(2)).setHint(SpecHints.HINT_SPEC_LOCK_TIMEOUT, Timeouts.SKIP_LOCKED_MILLI);
  }

  @Test
  void failureLimitIsCheckedBeforeIntent() {
    usePolicy(new PublicationPolicy(null, 1));
    row.failed(0, "IOException");
    var result = relay.runBatch();
    assertEquals(1, result.exhausted());
    assertEquals(1, result.discarded());
    assertFalse(row.publicationMayHaveOccurred());
    verifyNoInteractions(publisher);
  }

  @Test
  void failureThatReachesLimitCommitsAuditInsteadOfRetry() throws Exception {
    usePolicy(new PublicationPolicy(null, 1));
    doThrow(new IOException("private error")).when(publisher).publish(row);
    var result = relay.runBatch();
    assertEquals(1, result.exhausted());
    assertEquals(0, result.retries());
    assertEquals(
        1,
        registry.get("knd.publication.discards").tag("reason", "max_failures").counter().count());
    assertEquals(1, registry.get("knd.publication.discards.unknown").counter().count());
    assertEquals(1, registry.get("knd.publication.failures").counter().count());
    assertEquals(1, row.attempts());
    assertTrue(row.publicationMayHaveOccurred());
    verify(em).persist(any(DiscardAudit.class));
    verify(em).remove(row);
  }

  @Test
  void expiryAfterIntentIsRecheckedBeforeSend() {
    usePolicy(new PublicationPolicy(1, null));
    when(clock.getSingleResult()).thenReturn(999L, 1000L);
    assertEquals(1, relay.runBatch().expired());
    assertTrue(row.publicationMayHaveOccurred());
    verifyNoInteractions(publisher);
  }

  @Test
  void expiryDuringFailedSendWinsOverTheFailureLimit() throws Exception {
    usePolicy(new PublicationPolicy(1, 1));
    when(clock.getSingleResult()).thenReturn(999L, 999L, 999L, 1000L);
    doThrow(new IOException()).when(publisher).publish(row);
    var result = relay.runBatch();
    assertEquals(1, result.expired());
    assertEquals(0, result.exhausted());
  }

  @Test
  void expiryDuringSuccessfulSendDoesNotDiscardAcceptedMessage() throws Exception {
    usePolicy(new PublicationPolicy(1, null));
    when(clock.getSingleResult()).thenReturn(999L);
    org.mockito.Mockito.doAnswer(
            call -> {
              when(clock.getSingleResult()).thenReturn(1000L);
              return null;
            })
        .when(publisher)
        .publish(row);
    assertEquals(1, relay.runBatch().published());
    verify(em, never()).persist(any());
  }

  @Test
  void expiryScanningAndPublicationShareTheLimitAcrossSingleRowBatches() throws Exception {
    var head = row;
    usePolicy(new PublicationPolicy(1, null));
    var expiring = row;
    when(expired.getResultList()).thenReturn(List.of(expiring));
    when(query.getResultList()).thenReturn(List.of(head));
    when(prepared.getResultList()).thenReturn(List.of(head));
    relay =
        new OutboxRelay(
            work -> work.accept(em), publisher, BridgeConfig.from(Map.of("batch-size", "1")));
    assertEquals(1, relay.runBatch().expired());
    assertEquals(1, relay.runBatch().published());
    verify(publisher).publish(head);
    verify(publisher, never()).publish(expiring);
  }

  @Test
  void expiryScanFallsBackWhenNoHeadIsDue() {
    relay.runBatch();
    usePolicy(new PublicationPolicy(1, null));
    when(query.getResultList()).thenReturn(List.of());
    when(expired.getResultList()).thenReturn(List.of(row), List.of());
    assertEquals(1, relay.runBatch().expired());
  }

  @Test
  void failedAuditWriteOrCommitNeverCountsDiscard() {
    usePolicy(new PublicationPolicy(1, null));
    doThrow(new IllegalStateException("audit rejected")).when(em).persist(any(DiscardAudit.class));
    var result = relay.runBatch();
    assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, result.outcome());
    assertEquals(0, result.discarded());
    verify(em, never()).remove(any());
    verifyNoInteractions(publisher);
  }

  @Test
  void failedDiscardDefersOnlyTheUnchangedOriginalWithoutConsumingPublicationAttempts() {
    usePolicy(new PublicationPolicy(1, 1));
    when(clock.getSingleResult()).thenReturn(10_000L);
    doThrow(new IllegalStateException("audit rejected")).when(em).persist(any(DiscardAudit.class));
    assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, relay.runBatch().outcome());
    assertTrue(row.nextAttemptAt() >= 10_500);
    assertTrue(row.nextAttemptAt() <= 11_000);
    assertEquals(row.nextAttemptAt(), row.nextExpiryAttemptAt());
    assertEquals(0, row.attempts());
    assertEquals(1000L, row.expiresAt());
    assertNull(row.lastError());
    assertFalse(row.publicationMayHaveOccurred());
    verify(unchanged).setParameter("id", row.id());
    verify(unchanged).setParameter("version", row.version());
    verify(unchanged).setLockMode(LockModeType.PESSIMISTIC_WRITE);
    verify(unchanged).setHint(SpecHints.HINT_SPEC_LOCK_TIMEOUT, Timeouts.SKIP_LOCKED_MILLI);
    assertEquals(1, registry.get("knd.publication.transaction.failures").counter().count());
    assertEquals(0, registry.get("knd.publication.failures").counter().count());
    assertEquals(0, registry.get("knd.publication.retries").counter().count());
  }

  @Test
  void failedDiscardDoesNotChangeResolvedOrLockedOriginal() {
    usePolicy(new PublicationPolicy(1, null));
    when(unchanged.getResultList()).thenReturn(List.of());
    doThrow(new IllegalStateException("commit unknown")).when(em).persist(any(DiscardAudit.class));
    assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, relay.runBatch().outcome());
    assertEquals(0, row.nextAttemptAt());
    verify(unchanged).getResultList();
  }

  @Test
  void failedLookupAfterCommittedDiscardDoesNotBackOffThePreviousRow() {
    usePolicy(new PublicationPolicy(1, null));
    when(query.getResultList())
        .thenReturn(List.of(row))
        .thenThrow(new IllegalStateException("lookup failed"));
    var result = relay.runBatch();
    assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, result.outcome());
    assertEquals(1, result.discarded());
    verifyNoInteractions(unchanged);
  }

  @Test
  void failedDiscardCommitNeverCountsOrReleasesAnotherHead() {
    usePolicy(new PublicationPolicy(1, null));
    var worker =
        new OutboxRelay(
            work -> {
              work.accept(em);
              throw new IllegalStateException("commit unknown");
            },
            publisher,
            BridgeConfig.from(Map.of()));
    var result = worker.runBatch();
    assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, result.outcome());
    assertEquals(0, result.discarded());
    verify(query).getResultList();
    verifyNoInteractions(publisher);
  }

  @Test
  void committedDiscardStillCountsWhenShutdownFollowsCommit() {
    usePolicy(new PublicationPolicy(1, null));
    var ref = new java.util.concurrent.atomic.AtomicReference<OutboxRelay>();
    var worker =
        new OutboxRelay(
            work -> {
              work.accept(em);
              ref.get().stop();
            },
            publisher,
            BridgeConfig.from(Map.of()));
    ref.set(worker);
    assertEquals(1, worker.runBatch().discarded());
  }

  @Test
  void stopDuringEmptyExpiryLookupDoesNotTryAnotherQuery() {
    when(expired.getResultList())
        .thenAnswer(
            call -> {
              relay.stop();
              return List.of();
            });
    assertEquals(OutboxRelay.Outcome.STOPPED, relay.runBatch().outcome());
    verifyNoInteractions(query, prepared, publisher);
  }

  @Test
  void metricsCountOnlyCommittedDiscardsAndKeepFailuresSeparate() {
    usePolicy(new PublicationPolicy(1, null));
    var commits = new AtomicInteger();
    when(query.getResultList()).thenReturn(List.of(row), List.of(row), List.of());
    var worker =
        new OutboxRelay(
            work -> {
              work.accept(em);
              assertEquals(
                  0,
                  registry
                      .get("knd.publication.discards")
                      .tag("reason", "expired")
                      .counter()
                      .count());
              if (commits.getAndIncrement() == 0) {
                throw new IllegalStateException("commit rejected");
              }
            },
            publisher,
            BridgeConfig.from(Map.of("batch-size", "1")),
            metrics);
    assertEquals(0, worker.runBatch().discarded());
    assertEquals(1, registry.get("knd.publication.transaction.failures").counter().count());
    assertEquals(1, worker.runBatch().discarded());
    assertEquals(
        1, registry.get("knd.publication.discards").tag("reason", "expired").counter().count());
    assertEquals(0, registry.get("knd.publication.discards.unknown").counter().count());
    assertEquals(0, registry.get("knd.publication.failures").counter().count());
    assertTrue(
        registry.getMeters().stream()
            .allMatch(
                m -> m.getId().getTags().stream().allMatch(tag -> tag.getKey().equals("reason"))));
  }

  private void usePolicy(PublicationPolicy policy) {
    row =
        new OutboxEvent(
            "discard",
            "subject",
            "{}",
            0,
            "realm",
            "io.keycloak.user.login",
            new EventOrdering("realm", "user", 11),
            new ResolvedPublicationPolicy(policy, EventFilter.digest(new byte[0]), "rule"));
    when(query.getResultList()).thenReturn(List.of(row), List.of());
    when(prepared.getResultList()).thenReturn(List.of(row));
  }
}
