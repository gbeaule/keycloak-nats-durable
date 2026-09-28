package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.Config;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.KeycloakSessionTask;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.models.utils.KeycloakModelUtils;
import org.keycloak.models.utils.PostMigrationEvent;
import org.keycloak.provider.ProviderEvent;
import org.keycloak.provider.ProviderEventListener;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class DurableEventListenerFactoryTest {
  @Test
  void startsTheConfiguredWorkersOnlyOnceAndOnlyAfterMigration() throws Exception {
    try (var fixture = new Lifecycle()) {
      assertEquals("nats-durable", fixture.factory.getId());
      fixture.events.onEvent(mock(ProviderEvent.class));
      verifyNoInteractions(fixture.executor);
      assertEquals(0, fixture.publishers.constructed().size());
      fixture.start();
      fixture.start();
      assertEquals(2, fixture.publishers.constructed().size());
      assertEquals(2, fixture.relays.constructed().size());
      verify(fixture.cleanups.constructed().getFirst()).start();
      var tasks = ArgumentCaptor.forClass(Runnable.class);
      verify(fixture.executor, times(2)).execute(tasks.capture());
      tasks.getAllValues().forEach(task -> assertInstanceOf(RelayWorker.class, task));
      var threads = ArgumentCaptor.forClass(ThreadFactory.class);
      fixture.executors.verify(() -> Executors.newFixedThreadPool(eq(2), threads.capture()));
      Runnable work = mock(Runnable.class);
      Thread first = threads.getValue().newThread(work);
      Thread second = threads.getValue().newThread(work);
      assertTrue(first.isDaemon());
      assertTrue(second.isDaemon());
      assertEquals("keycloak-nats-outbox-1", first.getName());
      assertEquals("keycloak-nats-outbox-2", second.getName());
      first.run();
      verify(work).run();
      fixture.publishers.constructed().forEach(publisher -> verifyNoInteractions(publisher));
    }
  }

  @Test
  void requestCompletionSignalsAllWorkersWithoutClosingTheirResources() throws Exception {
    try (var fixture = new Lifecycle();
        var capture = mockStatic(CaptureRepository.class)) {
      fixture.start();
      var session = mock(KeycloakSession.class);
      var transaction = mock(KeycloakTransactionManager.class);
      var jpa = mock(JpaConnectionProvider.class);
      when(session.getTransactionManager()).thenReturn(transaction);
      when(transaction.isActive()).thenReturn(true);
      when(session.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
      when(jpa.getEntityManager()).thenReturn(mock(EntityManager.class));
      var listener = fixture.factory.create(session);
      listener.onEvent(EventEnvelopeTest.login());
      fixture.wakeups.constructed().forEach(wakeup -> verifyNoInteractions(wakeup));
      var completion = ArgumentCaptor.forClass(KeycloakTransaction.class);
      verify(transaction).enlistAfterCompletion(completion.capture());
      completion.getValue().begin();
      completion.getValue().commit();
      fixture.wakeups.constructed().forEach(wakeup -> verify(wakeup).signal());
      listener.close();
      fixture.publishers.constructed().forEach(publisher -> verifyNoInteractions(publisher));
    }
  }

  @Test
  void relayWorkUsesTheSessionOwnedByTheKeycloakTransactionRunner() throws Exception {
    try (var fixture = new Lifecycle();
        var jobs = mockStatic(KeycloakModelUtils.class)) {
      fixture.start();
      var session = mock(KeycloakSession.class);
      var jpa = mock(JpaConnectionProvider.class);
      var entityManager = mock(EntityManager.class);
      when(session.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
      when(jpa.getEntityManager()).thenReturn(entityManager);
      jobs.when(() -> KeycloakModelUtils.runJobInTransaction(eq(fixture.sessions), any()))
          .thenAnswer(
              invocation -> {
                KeycloakSessionTask task = invocation.getArgument(1);
                task.run(session);
                return null;
              });
      var received = new java.util.concurrent.atomic.AtomicReference<EntityManager>();
      fixture.transactions.getFirst().run(received::set);
      assertSame(entityManager, received.get());
      var failure = new IllegalStateException("database failed");
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () ->
                  fixture
                      .transactions
                      .getFirst()
                      .run(
                          actual -> {
                            throw failure;
                          })));
    }
  }

  @Test
  void cleanupUsesManagedSessionsAndTheConfiguredTransactionTimeout() throws Exception {
    try (var fixture = new Lifecycle();
        var jobs = mockStatic(KeycloakModelUtils.class)) {
      fixture.start();
      var session = mock(KeycloakSession.class);
      var jpa = mock(JpaConnectionProvider.class);
      var em = mock(EntityManager.class);
      when(session.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
      when(jpa.getEntityManager()).thenReturn(em);
      jobs.when(
              () ->
                  KeycloakModelUtils.runJobInTransactionWithTimeout(
                      eq(fixture.sessions), any(), eq(10)))
          .thenAnswer(
              call -> {
                KeycloakSessionTask task = call.getArgument(1);
                task.run(session);
                return null;
              });
      fixture.cleanupTransactions.getFirst().run(actual -> assertSame(em, actual));
      fixture.factory.close();
      verify(fixture.cleanups.constructed().getFirst()).close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shutdownStopsClaimsAndDrainsBeforeClosingConnections(boolean terminated) throws Exception {
    try (var fixture = new Lifecycle()) {
      fixture.start();
      when(fixture.executor.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(terminated);
      fixture.factory.close();
      fixture.factory.close();
      fixture.start();
      assertEquals(2, fixture.publishers.constructed().size());
      for (int i = 0; i < 2; i++) {
        var relay = fixture.relays.constructed().get(i);
        var wakeup = fixture.wakeups.constructed().get(i);
        var publisher = fixture.publishers.constructed().get(i);
        var order = inOrder(relay, wakeup, fixture.executor, publisher);
        order.verify(relay).stop();
        order.verify(wakeup).close();
        order.verify(fixture.executor).shutdown();
        order.verify(fixture.executor).awaitTermination(5, TimeUnit.SECONDS);
        if (!terminated) {
          order.verify(fixture.executor).shutdownNow();
        }
        order.verify(publisher).close();
      }
      var cleanup = fixture.cleanups.constructed().getFirst();
      var order = inOrder(cleanup, fixture.executor);
      order.verify(cleanup).stop();
      order.verify(fixture.executor).shutdown();
      order.verify(cleanup).close();
    }
  }

  @Test
  void interruptedShutdownPreservesInterruptAfterClosingConnections() throws Exception {
    try (var fixture = new Lifecycle()) {
      fixture.start();
      when(fixture.executor.awaitTermination(5, TimeUnit.SECONDS))
          .thenThrow(new InterruptedException());
      try {
        fixture.factory.close();
        assertTrue(Thread.currentThread().isInterrupted());
        fixture.publishers.constructed().forEach(publisher -> verify(publisher).close());
      } finally {
        Thread.interrupted();
      }
    }
  }

  @Test
  void closingBeforeMigrationPreventsLateStartup() throws Exception {
    try (var fixture = new Lifecycle()) {
      fixture.factory.close();
      fixture.start();
      fixture.executors.verifyNoInteractions();
      assertEquals(0, fixture.publishers.constructed().size());
    }
  }

  @Test
  void closingBeforeInitializationIsSafeAndIdempotent() {
    var factory = new DurableEventListenerFactory();
    factory.close();
    factory.close();
    assertEquals("nats-durable", factory.getId());
  }

  private static final class Lifecycle implements AutoCloseable {
    private final ExecutorService executor = mock(ExecutorService.class);
    private final KeycloakSessionFactory sessions = mock(KeycloakSessionFactory.class);
    private final List<OutboxRelay.Transactions> transactions = new ArrayList<>();
    private final List<OutboxRelay.Transactions> cleanupTransactions = new ArrayList<>();
    private final MockedConstruction<AuditCleanup> cleanups =
        mockConstruction(
            AuditCleanup.class,
            (cleanup, context) ->
                cleanupTransactions.add((OutboxRelay.Transactions) context.arguments().getFirst()));
    private final MockedStatic<Executors> executors = mockStatic(Executors.class);
    private final MockedConstruction<JetStreamPublisher> publishers =
        mockConstruction(JetStreamPublisher.class);
    private final MockedConstruction<RelayWakeup> wakeups = mockConstruction(RelayWakeup.class);
    private final MockedConstruction<OutboxRelay> relays =
        mockConstruction(
            OutboxRelay.class,
            (relay, context) ->
                transactions.add((OutboxRelay.Transactions) context.arguments().getFirst()));
    private final DurableEventListenerFactory factory = new DurableEventListenerFactory();
    private final ProviderEventListener events;

    Lifecycle() throws Exception {
      executors
          .when(() -> Executors.newFixedThreadPool(eq(2), any(ThreadFactory.class)))
          .thenReturn(executor);
      when(executor.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);
      var scope = mock(Config.Scope.class);
      when(scope.get(anyString(), nullable(String.class)))
          .thenAnswer(call -> Map.of("relay-workers", "2").get(call.getArgument(0)));
      factory.init(scope);
      factory.postInit(sessions);
      var registered = ArgumentCaptor.forClass(ProviderEventListener.class);
      verify(sessions).register(registered.capture());
      events = registered.getValue();
    }

    void start() {
      events.onEvent(new PostMigrationEvent(sessions));
    }

    @Override
    public void close() {
      try {
        factory.close();
      } finally {
        cleanups.close();
        relays.close();
        wakeups.close();
        publishers.close();
        executors.close();
      }
    }
  }
}
