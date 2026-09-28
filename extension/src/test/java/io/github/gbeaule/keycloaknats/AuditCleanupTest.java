package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.hibernate.Timeouts;
import org.hibernate.jpa.HibernateHints;
import org.hibernate.jpa.SpecHints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class AuditCleanupTest {
  private EntityManager em;
  private TypedQuery<DiscardAudit> selection;
  private TypedQuery<Object[]> statistics;
  private Query settings;
  private SimpleMeterRegistry registry;
  private AuditCleanup cleanup;
  private DiscardAudit audit;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setup() {
    em = mock(EntityManager.class);
    selection = mock(TypedQuery.class, RETURNS_SELF);
    statistics = mock(TypedQuery.class, RETURNS_SELF);
    settings = mock(Query.class, RETURNS_SELF);
    var clock = mock(Query.class);
    when(clock.getSingleResult()).thenReturn(900000000L);
    when(em.createNativeQuery(anyString(), eq(Long.class))).thenReturn(clock);
    when(em.createNativeQuery(anyString(), eq(Object[].class))).thenReturn(settings);
    when(em.createQuery(anyString(), eq(DiscardAudit.class))).thenReturn(selection);
    when(em.createQuery(anyString(), eq(Object[].class))).thenReturn(statistics);
    when(statistics.getSingleResult()).thenReturn(new Object[] {12L, 1000L});
    audit =
        new DiscardAudit(
            CaptureFixtures.row("old", "subject", "secret", 0), DiscardReason.EXPIRED, 1000);
    registry = new SimpleMeterRegistry();
  }

  @AfterEach
  void close() {
    Thread.interrupted();
    if (cleanup != null) {
      cleanup.close();
    }
    registry.close();
  }

  private AuditCleanup worker(OutboxRelay.Transactions transactions, Map<String, String> settings) {
    cleanup = new AuditCleanup(transactions, BridgeConfig.from(settings).auditCleanup(), registry);
    return cleanup;
  }

  @Test
  void deletesOldestEligibleRowsWithSkipLockedAndReportsOnlyCommittedWork() {
    when(selection.getResultList()).thenReturn(List.of(audit));
    var commits = new AtomicInteger();
    worker(
            work -> {
              work.accept(em);
              assertEquals(commits.getAndIncrement(), counter("deleted"));
              assertEquals(0, gauge("cleanup.last.success.timestamp.seconds"));
            },
            Map.of())
        .run();
    verify(selection).setParameter("cutoff", 295200000L);
    verify(selection).setMaxResults(500);
    verify(selection).setLockMode(LockModeType.PESSIMISTIC_WRITE);
    verify(selection).setHint(SpecHints.HINT_SPEC_LOCK_TIMEOUT, Timeouts.SKIP_LOCKED_MILLI);
    verify(em).remove(audit);
    verify(settings, times(2)).setParameter("timeout", "10000");
    assertEquals(1, counter("deleted"));
    assertEquals(12, gauge("retained.rows"));
    assertEquals(899999, gauge("oldest.eligible.age.seconds"));
    assertEquals(900000, gauge("cleanup.last.success.timestamp.seconds"));
    assertEquals(0, counter("failures"));
  }

  @Test
  void boundsTransactionsEvenWithFullBacklogAndZeroRetention() {
    when(selection.getResultList()).thenReturn(List.of(audit));
    var commits = new AtomicInteger();
    worker(
            work -> {
              work.accept(em);
              commits.incrementAndGet();
            },
            Map.of(
                "audit-retention-seconds",
                "0",
                "audit-cleanup-batch-size",
                "1",
                "audit-cleanup-max-batches",
                "2"))
        .run();
    assertEquals(3, commits.get()); // Two deletes and one metadata observation.
    verify(selection, times(2)).setParameter("cutoff", 900000000L);
    assertEquals(2, counter("deleted"));
  }

  @Test
  void emptySweepIsSuccessfulAndNeverResetsCounters() {
    when(selection.getResultList()).thenReturn(List.of(audit), List.of());
    worker(work -> work.accept(em), Map.of()).run();
    when(statistics.getSingleResult()).thenReturn(new Object[] {0L, null});
    cleanup.run();
    assertEquals(1, counter("deleted"));
    assertEquals(0, gauge("retained.rows"));
    assertEquals(0, gauge("oldest.eligible.age.seconds"));
  }

  @Test
  void failedCommitIsUncountedAndTheNextSweepRetries() {
    when(selection.getResultList()).thenReturn(List.of(audit));
    var attempts = new AtomicInteger();
    worker(
        work -> {
          work.accept(em);
          if (attempts.getAndIncrement() == 0) {
            throw new IllegalStateException("commit rejected");
          }
        },
        Map.of());
    assertTrue(Double.isNaN(gauge("retained.rows")));
    cleanup.run();
    assertEquals(0, counter("deleted"));
    assertEquals(1, counter("failures"));
    assertEquals(0, gauge("cleanup.last.success.timestamp.seconds"));
    cleanup.run();
    assertEquals(1, counter("deleted"));
    assertEquals(1, counter("failures"));
  }

  @Test
  void failureAfterAnEarlierCommitKeepsItsCountAndTheLastSuccessfulObservation() {
    worker(
            work -> work.accept(em),
            Map.of("audit-cleanup-batch-size", "1", "audit-cleanup-max-batches", "2"))
        .run();
    when(selection.getResultList())
        .thenReturn(List.of(audit))
        .thenThrow(new IllegalStateException("database unavailable"));
    cleanup.run();
    assertEquals(1, counter("deleted"));
    assertEquals(1, counter("failures"));
    assertEquals(900000, gauge("cleanup.last.success.timestamp.seconds"));
    assertEquals(12, gauge("retained.rows"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"settings", "selection", "flush"})
  void interruptionInsideTheTransactionForcesRollback(String phase) {
    when(selection.getResultList()).thenReturn(List.of(audit));
    switch (phase) {
      case "settings" ->
          when(settings.getSingleResult())
              .thenAnswer(
                  call -> {
                    Thread.currentThread().interrupt();
                    return null;
                  });
      case "selection" ->
          when(selection.getResultList())
              .thenAnswer(
                  call -> {
                    Thread.currentThread().interrupt();
                    return List.of(audit);
                  });
      case "flush" ->
          doAnswer(
                  call -> {
                    Thread.currentThread().interrupt();
                    return null;
                  })
              .when(em)
              .flush();
      default -> throw new AssertionError(phase);
    }
    // A real runner propagates the aborted transaction, rather than returning a result.
    worker(
            work -> {
              assertThrows(CancellationException.class, () -> work.accept(em));
              throw new CancellationException();
            },
            Map.of())
        .run();
    assertTrue(Thread.currentThread().isInterrupted());
    assertEquals(0, counter("deleted"));
    assertEquals(1, counter("failures"));
  }

  @Test
  void interruptionBetweenTransactionsPreventsAnotherClaim() {
    worker(
            work -> {
              work.accept(em);
              Thread.currentThread().interrupt();
            },
            Map.of())
        .run();
    verify(em, times(1)).flush();
    assertEquals(1, counter("failures"));
  }

  @Test
  void stoppedOrAlreadyInterruptedWorkersDoNoDatabaseWork() {
    worker(work -> work.accept(em), Map.of());
    Thread.currentThread().interrupt();
    cleanup.run();
    Thread.interrupted();
    cleanup.close();
    cleanup.run();
    cleanup.start();
    verifyNoInteractions(em);
    assertTrue(registry.getMeters().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"finished", "pending", "interrupted"})
  void schedulerStartsOnceAndStopsWithBoundedWaiting(String result) throws Exception {
    var executor = mock(ScheduledExecutorService.class);
    try (var executors = mockStatic(Executors.class)) {
      var threads = ArgumentCaptor.forClass(ThreadFactory.class);
      executors
          .when(() -> Executors.newSingleThreadScheduledExecutor(threads.capture()))
          .thenReturn(executor);
      worker(work -> work.accept(em), Map.of());
      cleanup.start();
      cleanup.start();
      var thread = threads.getValue().newThread(() -> {});
      assertEquals("keycloak-nats-audit-cleanup", thread.getName());
      assertTrue(thread.isDaemon());
      verify(executor).scheduleWithFixedDelay(cleanup, 0, 60000, TimeUnit.MILLISECONDS);
      if (result.equals("interrupted")) {
        when(executor.awaitTermination(5, TimeUnit.SECONDS)).thenThrow(new InterruptedException());
      } else {
        when(executor.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(result.equals("finished"));
      }
      cleanup.close();
      cleanup.close();
      assertEquals(result.equals("interrupted"), Thread.currentThread().isInterrupted());
      verify(executor).shutdownNow();
      assertTrue(registry.getMeters().isEmpty());
    }
  }

  @Test
  void inspectionUsesBoundedReadOnlyKeysetPagesAndOnlyMetadata() {
    when(selection.getResultList()).thenReturn(List.of(audit));
    var metadata = AuditRepository.inspect(em, 999, "previous", 1).getFirst();
    assertEquals("old", metadata.id());
    assertEquals(1000, metadata.discardedAt());
    assertEquals(DiscardReason.EXPIRED, metadata.reason());
    verify(selection).setHint(HibernateHints.HINT_READ_ONLY, true);
    verify(selection).setParameter("afterTime", 999L);
    verify(selection).setParameter("afterId", "previous");
    verify(selection).setMaxResults(1);
    verify(em, never()).flush();
  }

  @ParameterizedTest
  @CsvSource({"0,id", "501,id", "1,"})
  void inspectionRejectsUnboundedRequests(int limit, String cursor) {
    assertThrows(
        IllegalArgumentException.class, () -> AuditRepository.inspect(em, 0, cursor, limit));
    verifyNoInteractions(em);
  }

  private double counter(String suffix) {
    return registry.get("knd.audit.cleanup." + suffix).counter().count();
  }

  private double gauge(String suffix) {
    return registry.get("knd.audit." + suffix).gauge().value();
  }
}
