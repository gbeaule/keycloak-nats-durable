package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.utils.KeycloakModelUtils;
import org.keycloak.provider.Provider;
import org.keycloak.services.DefaultKeycloakContext;
import org.keycloak.services.DefaultKeycloakSession;
import org.keycloak.services.DefaultKeycloakSessionFactory;
import org.keycloak.tracing.TracingProvider;

class KeycloakTransactionsTest {
  @Test
  void returnsOnlyAfterCommitAndUsesFreshSessionsForEachCall() {
    var fixture = new Lifecycle();
    fixture.transactions.run(
        em -> {
          assertSame(fixture.em, em);
          assertEquals(0, fixture.commits.get());
        });
    assertEquals(1, fixture.commits.get());
    fixture.transactions.run(em -> assertEquals(1, fixture.commits.get()));
    assertEquals(2, fixture.commits.get());
    assertEquals(2, fixture.sessions.get());
    assertEquals(0, fixture.rollbacks.get());
  }

  @Test
  void rollbackOnlyAtSessionCloseCannotReusePreviousCommitConfirmation() {
    var fixture = new Lifecycle();
    fixture.transactions.run(em -> {});
    fixture.rollbackSession = 2;
    var failure =
        assertThrows(IllegalStateException.class, () -> fixture.transactions.run(em -> {}));
    assertEquals("Keycloak transaction commit was not confirmed", failure.getMessage());
    assertEquals(1, fixture.commits.get());
    assertEquals(1, fixture.rollbacks.get());
    fixture.transactions.run(em -> {});
    assertEquals(2, fixture.commits.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"work", "commit", "completion"})
  void preservesFailuresFromWorkCommitAndCompletion(String phase) {
    var fixture = new Lifecycle();
    var failure = new IllegalStateException("injected failure");
    if (!phase.equals("work")) {
      fixture.beforeClose =
          session -> {
            var failing =
                new AbstractKeycloakTransaction() {
                  @Override
                  protected void commitImpl() {
                    throw failure;
                  }

                  @Override
                  protected void rollbackImpl() {}
                };
            if (phase.equals("commit")) {
              session.getTransactionManager().enlist(failing);
            } else {
              session.getTransactionManager().enlistAfterCompletion(failing);
            }
          };
    }
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                fixture.transactions.run(
                    em -> {
                      if (phase.equals("work")) {
                        throw failure;
                      }
                    })));
  }

  @Test
  void runnerWithoutTransactionCompletionCannotReportSuccess() {
    var transactions = new KeycloakTransactions(task -> {});
    assertThrows(IllegalStateException.class, () -> transactions.run(em -> {}));
  }

  @ParameterizedTest
  @ValueSource(strings = {"intent", "publication", "discard"})
  void silentRollbackLeavesRelayResolutionUncountedAndBacksOffFailedDiscard(String phase)
      throws Exception {
    var fixture = new Lifecycle();
    fixture.rollbackSession = phase.equals("publication") ? 2 : 1;
    var publisher = mock(EventPublisher.class);
    boolean discard = phase.equals("discard");
    var row =
        new OutboxEvent(
            "id",
            "subject",
            "{}",
            -2000,
            "realm",
            "io.keycloak.user.login",
            null,
            new ResolvedPublicationPolicy(
                discard ? new PublicationPolicy(1, null) : PublicationPolicy.RETRY,
                EventFilter.digest(new byte[0]),
                null));
    var registry = new SimpleMeterRegistry();
    try (var repository = mockStatic(OutboxRepository.class);
        var clock = mockStatic(CaptureRepository.class);
        var metrics = new RelayMetrics(registry)) {
      repository
          .when(() -> OutboxRepository.lockNextDue(fixture.em, 0))
          .thenReturn(Optional.of(row));
      repository
          .when(() -> OutboxRepository.lockPrepared(fixture.em, row.id(), 0, 0))
          .thenReturn(Optional.of(row));
      repository
          .when(() -> OutboxRepository.lockUnchanged(fixture.em, row.id(), 0))
          .thenReturn(Optional.of(row));
      var relay =
          new OutboxRelay(
              fixture.transactions,
              publisher,
              BridgeConfig.from(Map.of("batch-size", "1")),
              metrics);
      var result = relay.runBatch();
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, result.outcome());
      assertEquals(0, result.published());
      assertEquals(0, result.discarded());
      assertEquals(0, result.retries());
      assertEquals(0, registry.get("knd.publication.confirmed").counter().count());
      assertEquals(
          0, registry.get("knd.publication.discards").tag("reason", "expired").counter().count());
      assertEquals(1, registry.get("knd.publication.transaction.failures").counter().count());
      assertEquals(1, fixture.rollbacks.get());
      if (phase.equals("publication")) {
        verify(publisher).publish(row);
        verify(fixture.em).remove(row);
      } else {
        verifyNoInteractions(publisher);
      }
      if (discard) {
        repository.verify(
            () -> OutboxRepository.discard(fixture.em, row, DiscardReason.EXPIRED, 0));
        repository.verify(() -> OutboxRepository.lockUnchanged(fixture.em, row.id(), 0));
        assertTrue(row.nextAttemptAt() > 0);
        assertTrue(row.nextExpiryAttemptAt() > 0);
        assertEquals(1, fixture.commits.get(), "Backoff must commit in a fresh transaction");
      } else {
        repository.verify(() -> OutboxRepository.lockUnchanged(fixture.em, row.id(), 0), never());
      }
    } finally {
      registry.close();
    }
  }

  @Test
  void auditCleanupDoesNotCountRolledBackDeletesAndRetriesTheNextSweep() {
    var fixture = new Lifecycle();
    fixture.rollbackSession = 1;
    var config = BridgeConfig.from(Map.of()).auditCleanup();
    var audit =
        new DiscardAudit(CaptureFixtures.row("id", "subject", "{}", 0), DiscardReason.EXPIRED, 0);
    var registry = new SimpleMeterRegistry();
    try (var repository = mockStatic(AuditRepository.class);
        var clock = mockStatic(CaptureRepository.class);
        var cleanup = new AuditCleanup(fixture.transactions, config, registry)) {
      repository
          .when(
              () ->
                  AuditRepository.lockExpired(
                      fixture.em, -config.retention().toMillis(), config.batchSize()))
          .thenReturn(List.of(audit));
      repository
          .when(() -> AuditRepository.statistics(fixture.em, config.retention()))
          .thenReturn(new AuditRepository.Statistics(0, 0, 1000));
      cleanup.run();
      assertEquals(0, registry.get("knd.audit.cleanup.deleted").counter().count());
      assertEquals(1, registry.get("knd.audit.cleanup.failures").counter().count());
      assertEquals(
          0, registry.get("knd.audit.cleanup.last.success.timestamp.seconds").gauge().value());
      assertEquals(1, fixture.rollbacks.get());
      cleanup.run();
      assertEquals(1, registry.get("knd.audit.cleanup.deleted").counter().count());
      assertEquals(1, registry.get("knd.audit.cleanup.failures").counter().count());
      assertEquals(
          1, registry.get("knd.audit.cleanup.last.success.timestamp.seconds").gauge().value());
    } finally {
      registry.close();
    }
  }

  /**
   * Real Keycloak transaction runner, session close and transaction manager; database work is
   * mocked.
   */
  private static final class Lifecycle {
    private final EntityManager em = mock(EntityManager.class);
    private final DefaultKeycloakSessionFactory factory = mock(DefaultKeycloakSessionFactory.class);
    private final AtomicInteger sessions = new AtomicInteger();
    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicInteger rollbacks = new AtomicInteger();
    private final KeycloakTransactions transactions =
        new KeycloakTransactions(task -> KeycloakModelUtils.runJobInTransaction(factory, task));
    private int rollbackSession;
    private Consumer<KeycloakSession> beforeClose = session -> {};

    Lifecycle() {
      var jpa = mock(JpaConnectionProvider.class);
      when(jpa.getEntityManager()).thenReturn(em);
      var tracing = mock(TracingProvider.class);
      doAnswer(
              call -> {
                Consumer<?> action = call.getArgument(2);
                action.accept(null);
                return null;
              })
          .when(tracing)
          .trace(any(Class.class), any(String.class), any(Consumer.class));
      when(factory.create())
          .thenAnswer(
              call -> {
                int number = sessions.incrementAndGet();
                var session =
                    new DefaultKeycloakSession(factory) {
                      @Override
                      protected DefaultKeycloakContext createKeycloakContext(
                          KeycloakSession owner) {
                        return mock(DefaultKeycloakContext.class);
                      }

                      @Override
                      public <T extends Provider> T getProvider(Class<T> type) {
                        if (type == TracingProvider.class) {
                          return type.cast(tracing);
                        }
                        if (type == JpaConnectionProvider.class) {
                          return type.cast(jpa);
                        }
                        return super.getProvider(type);
                      }

                      @Override
                      public void close() {
                        beforeClose.accept(this);
                        if (number == rollbackSession) {
                          getTransactionManager().setRollbackOnly();
                        }
                        super.close();
                      }
                    };
                session
                    .getTransactionManager()
                    .enlistAfterCompletion(
                        new AbstractKeycloakTransaction() {
                          @Override
                          protected void commitImpl() {
                            commits.incrementAndGet();
                          }

                          @Override
                          protected void rollbackImpl() {
                            rollbacks.incrementAndGet();
                          }
                        });
                return session;
              });
    }
  }
}
