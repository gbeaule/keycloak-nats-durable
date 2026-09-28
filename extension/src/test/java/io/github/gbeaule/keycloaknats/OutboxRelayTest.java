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

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.TypedQuery;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import org.hibernate.Timeouts;
import org.hibernate.jpa.SpecHints;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxRelayTest {
  private EntityManager em;
  private TypedQuery<OutboxEvent> query;
  private TypedQuery<OutboxEvent> prepared;
  private EventPublisher publisher;
  private OutboxEvent row;
  private OutboxRelay relay;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setup() {
    em = mock(EntityManager.class);
    publisher = mock(EventPublisher.class);
    query = mock(TypedQuery.class, RETURNS_SELF);
    prepared = mock(TypedQuery.class, RETURNS_SELF);
    when(em.createQuery(anyString(), eq(OutboxEvent.class)))
        .thenAnswer(call -> call.<String>getArgument(0).contains(":version") ? prepared : query);
    row = CaptureFixtures.row("id", "subject", "{}", 0);
    when(query.getResultList()).thenReturn(List.of(row), List.of());
    when(prepared.getResultList()).thenReturn(List.of(row));
    relay = new OutboxRelay(work -> work.accept(em), publisher, BridgeConfig.from(Map.of()));
  }

  @Test
  void removesOnlyAfterSuccessfulAcknowledgement() throws Exception {
    assertEquals(1, relay.runBatch().published());
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
      assertEquals(0, relay.runBatch().published());
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
            BridgeConfig.from(Map.of()));
    assertEquals(0, worker.runBatch().published());
    assertEquals(1, worker.runBatch().published());
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
    assertEquals(0, relay.runBatch().published());
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
        new OutboxRelay.BatchResult(2, 0, 0, OutboxRelay.Outcome.STALE), worker.runBatch());
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
        new OutboxRelay.BatchResult(2, 1, 1, OutboxRelay.Outcome.PUBLISHED), worker.runBatch());
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
}
