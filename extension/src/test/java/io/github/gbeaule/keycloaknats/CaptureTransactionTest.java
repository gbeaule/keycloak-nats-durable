package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import jakarta.persistence.EntityManager;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.hibernate.engine.spi.SessionImplementor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.common.ClientConnection;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.EventBuilder;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.ClientModel;
import org.keycloak.models.ClientProvider;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserProvider;
import org.keycloak.services.DefaultKeycloakTransactionManager;
import org.keycloak.services.resources.admin.AdminAuth;
import org.keycloak.services.resources.admin.AdminEventBuilder;
import org.keycloak.tracing.TracingProvider;
import org.keycloak.transaction.JtaTransactionWrapper;
import org.mockito.MockedStatic;

class CaptureTransactionTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void swallowedAdminLookupFailuresAbortExplicitCommitIncludingJta(boolean fatal) throws Exception {
    try (var fixture = new Fixture()) {
      var jta = mock(TransactionManager.class);
      when(jta.getStatus()).thenReturn(Status.STATUS_ACTIVE);
      fixture.tx.enlist(new JtaTransactionWrapper(fixture.session, jta));
      fixture.tx.begin();
      Throwable failure =
          fatal
              ? new AssertionError("remote storage failed")
              : new IllegalStateException("remote storage failed");
      when(fixture.user.isEnabled()).thenThrow(failure);
      fixture
          .admin()
          .resource(ResourceType.USER)
          .operation(OperationType.UPDATE)
          .resourcePath("users", Fixture.USER_ID)
          .success();

      assertTrue(
          fixture.tx.getRollbackOnly(), "The real builder must have swallowed a callback failure");
      var rejection = assertThrows(IllegalStateException.class, fixture.tx::commit);
      assertSame(failure, rejection.getCause());
      verify(fixture.resource).rollback();
      verify(fixture.resource, never()).commit();
      verify(jta).rollback();
      verify(jta, never()).commit();
      verifyNoInteractions(fixture.em, fixture.wakeup);
      assertTrue(fixture.rows.isEmpty());
    }
  }

  @Test
  void providerInitializationFailureCannotCommitEarlierAccountWork() {
    try (var fixture = new Fixture()) {
      var failure = new IllegalStateException("JPA provider failed");
      when(fixture.session.getProvider(JpaConnectionProvider.class)).thenThrow(failure);
      fixture.tx.begin();
      fixture
          .admin()
          .resource(ResourceType.USER)
          .operation(OperationType.DELETE)
          .resourcePath("users", Fixture.USER_ID)
          .success();
      assertSame(failure, assertThrows(IllegalStateException.class, fixture.tx::commit).getCause());
      verify(fixture.resource).rollback();
      verify(fixture.resource, never()).commit();
      verifyNoInteractions(fixture.em, fixture.wakeup);
    }
  }

  @Test
  void captureFailureInAnotherListenerAbortsAlreadyBufferedEventsAndKeepsTheOriginalCause() {
    try (var fixture = new Fixture()) {
      fixture.tx.begin();
      fixture.listener().onEvent(EventEnvelopeTest.login());
      var firstFailure = new IllegalStateException("remote storage failed");
      when(fixture.user.isEnabled()).thenThrow(firstFailure);
      fixture
          .admin()
          .resource(ResourceType.USER)
          .operation(OperationType.UPDATE)
          .resourcePath("users", Fixture.USER_ID)
          .success();
      assertThrows(
          IllegalStateException.class, () -> fixture.listener().onEvent(EventEnvelopeTest.login()));
      assertSame(
          firstFailure, assertThrows(IllegalStateException.class, fixture.tx::commit).getCause());
      verify(fixture.resource).rollback();
      verify(fixture.resource, never()).commit();
      verifyNoInteractions(fixture.em, fixture.wakeup);
    }
  }

  @Test
  void swallowedCallbackDuringFlushStillRejectsTheOwningCommit() {
    try (var fixture = new Fixture()) {
      doAnswer(
              call -> {
                try {
                  fixture.listener().onEvent(EventEnvelopeTest.login());
                } catch (IllegalStateException swallowedByKeycloak) {
                  // A model callback can catch this the same way Keycloak's event builders do.
                }
                return null;
              })
          .when(fixture.em)
          .flush();
      fixture.tx.begin();
      fixture.listener().onEvent(EventEnvelopeTest.login());
      var failure = assertThrows(IllegalStateException.class, fixture.tx::commit);
      assertEquals(
          "Event capture has already prepared for commit", failure.getCause().getMessage());
      verify(fixture.resource).rollback();
      verify(fixture.resource, never()).commit();
      verifyNoInteractions(fixture.wakeup);
    }
  }

  @Test
  void laterPrepareCallbacksMarkTheManagedTransactionAndPreventJtaCommit() throws Exception {
    try (var fixture = new Fixture()) {
      var marked = new AtomicBoolean();
      var jta = mock(TransactionManager.class);
      when(jta.getStatus()).thenReturn(Status.STATUS_ACTIVE);
      doAnswer(
              call -> {
                marked.set(true);
                return null;
              })
          .when(jta)
          .setRollbackOnly();
      doAnswer(
              call -> {
                if (marked.get()) {
                  throw new RollbackException("Transaction marked for rollback");
                }
                return null;
              })
          .when(jta)
          .commit();
      // Model the Hibernate JTA coordinator's delegation to the owning transaction manager.
      doAnswer(
              call -> {
                jta.setRollbackOnly();
                return null;
              })
          .when(fixture.managedTransaction)
          .markRollbackOnly();
      fixture.tx.enlist(new JtaTransactionWrapper(fixture.session, jta));
      fixture.tx.begin();
      fixture.listener().onEvent(EventEnvelopeTest.login());
      fixture.tx.enlistPrepare(
          new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
              fixture
                  .admin()
                  .resource(ResourceType.USER)
                  .operation(OperationType.UPDATE)
                  .resourcePath("users", Fixture.USER_ID)
                  .success();
            }

            @Override
            protected void rollbackImpl() {}
          });
      var failure = assertThrows(RuntimeException.class, fixture.tx::commit);
      assertTrue(failure.getCause() instanceof RollbackException);
      verify(jta).setRollbackOnly();
      verify(jta).commit();
      verify(fixture.managedTransaction).markRollbackOnly();
      verifyNoInteractions(fixture.wakeup);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void firstCallbackDuringPreparationMarksTheOwningTransactionWithoutReplacingItsFailure(
      boolean observationFails) throws Exception {
    try (var fixture = new Fixture()) {
      var marked = new AtomicBoolean();
      var jta = mock(TransactionManager.class);
      when(jta.getStatus()).thenReturn(Status.STATUS_ACTIVE);
      doAnswer(
              call -> {
                marked.set(true);
                return null;
              })
          .when(jta)
          .setRollbackOnly();
      doAnswer(
              call -> {
                if (marked.get()) {
                  throw new RollbackException("Transaction marked for rollback");
                }
                return null;
              })
          .when(jta)
          .commit();
      doAnswer(
              call -> {
                jta.setRollbackOnly();
                return null;
              })
          .when(fixture.managedTransaction)
          .markRollbackOnly();
      var observerFailure = new IllegalStateException("remote storage failed");
      if (observationFails) {
        when(fixture.user.isEnabled()).thenThrow(observerFailure);
      }
      var callbackFailure = new AtomicReference<RuntimeException>();
      fixture.tx.enlist(new JtaTransactionWrapper(fixture.session, jta));
      fixture.tx.begin();
      fixture.tx.enlistPrepare(
          new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
              var event = EventEnvelopeTest.admin(OperationType.UPDATE);
              event.setRealmId(Fixture.REALM_ID);
              event.setResourcePath("users/" + Fixture.USER_ID);
              try {
                fixture.listener().onEvent(event, false);
              } catch (RuntimeException swallowedByKeycloak) {
                callbackFailure.set(swallowedByKeycloak);
              }
            }

            @Override
            protected void rollbackImpl() {}
          });
      assertTrue(
          assertThrows(RuntimeException.class, fixture.tx::commit).getCause()
              instanceof RollbackException);
      if (observationFails) {
        assertSame(observerFailure, callbackFailure.get());
      } else {
        assertEquals("Transaction already completed", callbackFailure.get().getMessage());
      }
      assertTrue(marked.get());
      verify(fixture.managedTransaction).markRollbackOnly();
      verify(fixture.em, never()).persist(any());
      verifyNoInteractions(fixture.wakeup);
    }
  }

  @Test
  void repeatedRollbackMarkingFailureCannotSuppressItselfOrReplaceTheOriginalCause() {
    try (var fixture = new Fixture()) {
      var failure = new IllegalStateException("remote storage failed");
      doThrow(failure).when(fixture.managedTransaction).markRollbackOnly();
      when(fixture.user.isEnabled()).thenThrow(failure);
      fixture.tx.begin();
      fixture.listener().onEvent(EventEnvelopeTest.login());
      fixture.tx.enlistPrepare(
          new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
              fixture
                  .admin()
                  .resource(ResourceType.USER)
                  .operation(OperationType.UPDATE)
                  .resourcePath("users", Fixture.USER_ID)
                  .success();
            }

            @Override
            protected void rollbackImpl() {}
          });
      assertSame(failure, assertThrows(IllegalStateException.class, fixture.tx::commit).getCause());
      assertEquals(0, failure.getSuppressed().length);
      verifyNoInteractions(fixture.wakeup);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rollbackMarkingFailuresDoNotReplaceTheOriginalCaptureFailure(boolean fatal) {
    try (var fixture = new Fixture()) {
      Throwable markingFailure =
          fatal ? new AssertionError("mark failed") : new IllegalStateException("mark failed");
      doThrow(markingFailure).when(fixture.managedTransaction).markRollbackOnly();
      fixture.tx.begin();
      fixture.listener().onEvent(EventEnvelopeTest.login());
      fixture.tx.enlistPrepare(
          new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
              assertThrows(
                  IllegalStateException.class,
                  () -> fixture.listener().onEvent(EventEnvelopeTest.login()));
            }

            @Override
            protected void rollbackImpl() {}
          });
      var failure = assertThrows(IllegalStateException.class, fixture.tx::commit);
      assertEquals(
          "Event capture has already prepared for commit", failure.getCause().getMessage());
      assertEquals(List.of(markingFailure), List.of(failure.getCause().getSuppressed()));
      verifyNoInteractions(fixture.wakeup);
    }
  }

  @Test
  void laterCallbacksKeepTheFirstFailureWhenProviderInitializationHasAlreadyAbortedPreparation() {
    try (var fixture = new Fixture()) {
      var failure = new IllegalStateException("JPA provider failed");
      when(fixture.session.getProvider(JpaConnectionProvider.class)).thenThrow(failure);
      fixture.tx.begin();
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () -> fixture.listener().onEvent(EventEnvelopeTest.login())));
      fixture.tx.enlistPrepare(
          new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
              assertThrows(
                  IllegalStateException.class,
                  () -> fixture.listener().onEvent(EventEnvelopeTest.login()));
            }

            @Override
            protected void rollbackImpl() {}
          });
      assertSame(failure, assertThrows(IllegalStateException.class, fixture.tx::commit).getCause());
      verify(fixture.resource).rollback();
      verify(fixture.resource, never()).commit();
      verifyNoInteractions(fixture.managedTransaction, fixture.em, fixture.wakeup);
    }
  }

  @Test
  void realUserAndAdminBuildersShareOneOrderedBatchAndOneCompletionNotification() throws Exception {
    try (var fixture = new Fixture()) {
      fixture.tx.begin();
      var events = new EventBuilder(fixture.realm, fixture.session).user(fixture.user);
      events.event(EventType.UPDATE_PROFILE).success();
      fixture
          .admin()
          .resource(ResourceType.USER)
          .operation(OperationType.UPDATE)
          .resourcePath("users", Fixture.USER_ID)
          .success();
      events.event(EventType.UPDATE_EMAIL).success();
      fixture.tx.commit();

      var mapper = new ObjectMapper();
      var operations = new ArrayList<String>();
      for (var row : fixture.rows) {
        var data = mapper.readTree(row.payload()).path("data");
        operations.add(
            data.has("eventType")
                ? data.path("eventType").asText()
                : data.path("operationType").asText());
      }
      assertEquals(List.of("UPDATE_PROFILE", "UPDATE", "UPDATE_EMAIL"), operations);
      assertEquals(
          List.of(1L, 2L, 3L), fixture.rows.stream().map(OutboxEvent::userSequence).toList());
      verify(fixture.em, times(2)).flush();
      fixture.captures.verify(
          () ->
              CaptureRepository.allocateSequenceRange(
                  fixture.em, Fixture.REALM_ID, Fixture.USER_ID, 3));
      fixture.captures.verify(() -> CaptureRepository.readDatabaseTime(fixture.em));
      fixture.heads.verify(() -> OutboxHeads.append(fixture.em, fixture.rows.getFirst()));
      verify(fixture.wakeup).run();
      verify(fixture.resource).commit();
      verify(fixture.resource, never()).rollback();
    }
  }

  @Test
  void eachListenerRetainsItsOwnSerializationLimitWhenJoiningTheSharedBatch() {
    try (var fixture = new Fixture()) {
      fixture.tx.begin();
      var event = EventEnvelopeTest.login();
      event.setClientId("用".repeat(255));
      fixture.listener().onEvent(event);
      var stricter =
          new DurableEventListener(
              fixture.session,
              BridgeConfig.from(Map.of("max-payload-bytes", "1024")),
              fixture.wakeup);
      stricter.onEvent(event);
      var failure = assertThrows(IllegalArgumentException.class, fixture.tx::commit);
      assertEquals("Event exceeds max-payload-bytes", failure.getMessage());
      assertEquals(
          1,
          fixture.rows.size(),
          "The first listener's larger limit still applies to its snapshot");
      verify(fixture.resource).rollback();
      verify(fixture.resource, never()).commit();
      verifyNoInteractions(fixture.wakeup);
    }
  }

  @Test
  void excludedBuildersNeitherEnlistCaptureNorInitializeJpa() throws Exception {
    try (var fixture = new Fixture()) {
      fixture.tx.begin();
      var filter = EventFilterTest.parse("{\"userEvents\":[],\"adminEvents\":[]}");
      var listener =
          new DurableEventListener(fixture.session, fixture.config, fixture.wakeup, () -> filter);
      listener.onEvent(EventEnvelopeTest.login());
      listener.onEvent(EventEnvelopeTest.admin(OperationType.UPDATE), false);
      fixture.tx.commit();
      verify(fixture.session, never()).getProvider(JpaConnectionProvider.class);
      verifyNoInteractions(fixture.em, fixture.wakeup);
      assertTrue(fixture.attributes.isEmpty());
      verify(fixture.resource).commit();
    }
  }

  @Test
  void anInactiveCallbackDoesNotPoisonTheSessionsNextTransaction() {
    try (var fixture = new Fixture()) {
      var listener = fixture.listener();
      assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
      assertFalse(fixture.tx.getRollbackOnly());
      fixture.tx.begin();
      assertFalse(fixture.tx.getRollbackOnly());
      listener.onEvent(EventEnvelopeTest.login());
      fixture.tx.commit();
      assertEquals(1, fixture.rows.size());
      verify(fixture.resource).commit();
      verify(fixture.wakeup).run();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void keycloakRollsBackTheOwningResourceWhenPrepareCannotPersist(boolean fatal) {
    var session = mock(KeycloakSession.class);
    var em = mock(EntityManager.class);
    var jpa = mock(JpaConnectionProvider.class);
    var tracing = mock(TracingProvider.class);
    doAnswer(
            call -> {
              ((Consumer<?>) call.getArgument(2)).accept(null);
              return null;
            })
        .when(tracing)
        .trace(any(Class.class), any(String.class), any(Consumer.class));
    when(session.getProvider(TracingProvider.class)).thenReturn(tracing);
    when(session.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
    when(jpa.getEntityManager()).thenReturn(em);
    var tx = new DefaultKeycloakTransactionManager(session);
    tx.setJTAPolicy(KeycloakTransactionManager.JTAPolicy.NOT_SUPPORTED);
    when(session.getTransactionManager()).thenReturn(tx);
    var resource = mock(KeycloakTransaction.class);
    tx.enlist(resource);
    tx.begin();
    var wakeup = mock(Runnable.class);
    Throwable failure =
        fatal ? new AssertionError("capture failed") : new IllegalStateException("capture failed");
    doThrow(failure).when(em).persist(any());
    try (var capture = mockStatic(CaptureRepository.class)) {
      capture.when(() -> CaptureRepository.persistBatch(any(), any())).thenCallRealMethod();
      var event = EventEnvelopeTest.login();
      event.setUserId(null);
      new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeup).onEvent(event);
      verifyNoInteractions(em);
      var thrown = assertThrows(RuntimeException.class, tx::commit);
      assertSame(failure, fatal ? thrown.getCause() : thrown);
      verify(resource).rollback();
      org.mockito.Mockito.verify(resource, org.mockito.Mockito.never()).commit();
      verifyNoInteractions(wakeup);
    }
  }

  @SuppressWarnings("unchecked")
  private static final class Fixture implements AutoCloseable {
    static final String REALM_ID = "12345678-1234-1234-1234-123456789012";
    static final String USER_ID = "22345678-1234-1234-1234-123456789012";
    final KeycloakSession session = mock(KeycloakSession.class);
    final DefaultKeycloakTransactionManager tx = new DefaultKeycloakTransactionManager(session);
    final KeycloakTransaction resource = mock(KeycloakTransaction.class);
    final EntityManager em = mock(EntityManager.class);
    final org.hibernate.Transaction managedTransaction = mock(org.hibernate.Transaction.class);
    final Runnable wakeup = mock(Runnable.class);
    final RealmModel realm = mock(RealmModel.class);
    final UserModel user = mock(UserModel.class);
    final BridgeConfig config = BridgeConfig.from(Map.of());
    final Map<String, Object> attributes = new HashMap<>();
    final List<OutboxEvent> rows = new ArrayList<>();
    final MockedStatic<CaptureRepository> captures = mockStatic(CaptureRepository.class);
    final MockedStatic<OutboxHeads> heads = mockStatic(OutboxHeads.class);

    Fixture() {
      when(session.getAttribute(anyString()))
          .thenAnswer(call -> attributes.get(call.getArgument(0)));
      doAnswer(call -> attributes.put(call.getArgument(0), call.getArgument(1)))
          .when(session)
          .setAttribute(anyString(), any());
      when(session.getTransactionManager()).thenReturn(tx);
      tx.setJTAPolicy(KeycloakTransactionManager.JTAPolicy.NOT_SUPPORTED);
      tx.enlist(resource);
      var jpa = mock(JpaConnectionProvider.class);
      when(session.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
      when(jpa.getEntityManager()).thenReturn(em);
      var hibernate = mock(SessionImplementor.class);
      when(em.unwrap(SessionImplementor.class)).thenReturn(hibernate);
      when(hibernate.accessTransaction()).thenReturn(managedTransaction);
      doAnswer(
              call -> {
                rows.add(call.getArgument(0));
                return null;
              })
          .when(em)
          .persist(any());
      captures.when(() -> CaptureRepository.persistBatch(any(), any())).thenCallRealMethod();
      captures.when(() -> CaptureRepository.readDatabaseTime(any())).thenReturn(1234L);
      var positions = new HashMap<String, Long>();
      captures
          .when(() -> CaptureRepository.allocateSequenceRange(any(), any(), any(), anyInt()))
          .thenAnswer(
              call -> {
                // Account writes have flushed before the first managed-JDBC counter allocation.
                verify(em).flush();
                assertTrue(rows.isEmpty(), "All counter ranges precede outbox writes");
                String realmId = call.getArgument(1);
                String userId = call.getArgument(2);
                int count = call.getArgument(3);
                if (userId == null) {
                  return null;
                }
                long last =
                    positions.merge(EventOrdering.keyFor(realmId, userId), (long) count, Long::sum);
                return new EventOrdering(realmId, userId, last - count + 1);
              });
      var tracing = mock(TracingProvider.class);
      doAnswer(
              call -> {
                ((Consumer<?>) call.getArgument(2)).accept(null);
                return null;
              })
          .when(tracing)
          .trace(any(Class.class), any(String.class), any(Consumer.class));
      when(tracing.getCurrentSpan()).thenReturn(mock(Span.class));
      when(session.getProvider(TracingProvider.class)).thenReturn(tracing);
      var realms = mock(RealmProvider.class);
      var users = mock(UserProvider.class);
      when(session.realms()).thenReturn(realms);
      when(session.users()).thenReturn(users);
      when(session.clients()).thenReturn(mock(ClientProvider.class));
      when(realms.getRealm(REALM_ID)).thenReturn(realm);
      when(users.getUserById(realm, USER_ID)).thenReturn(user);
      when(user.getId()).thenReturn(USER_ID);
      when(user.isEnabled()).thenReturn(true);
      when(realm.getId()).thenReturn(REALM_ID);
      when(realm.getName()).thenReturn("durable-test");
      when(realm.getEventsListenersStream()).thenAnswer(call -> Stream.of("nats-durable"));
      when(realm.getEnabledEventTypesStream()).thenAnswer(call -> Stream.empty());
      var context = mock(KeycloakContext.class);
      when(context.getRealm()).thenReturn(realm);
      when(session.getContext()).thenReturn(context);
      var factory = mock(KeycloakSessionFactory.class);
      when(session.getKeycloakSessionFactory()).thenReturn(factory);
      var provider = mock(EventListenerProviderFactory.class);
      when(provider.getId()).thenReturn("nats-durable");
      when(provider.create(session)).thenAnswer(call -> listener());
      when(factory.getProviderFactoriesStream(EventListenerProvider.class))
          .thenAnswer(call -> Stream.of(provider));
      when(session.getProvider(EventListenerProvider.class, "nats-durable")).thenReturn(listener());
    }

    DurableEventListener listener() {
      return new DurableEventListener(session, config, wakeup);
    }

    AdminEventBuilder admin() {
      var auth = mock(AdminAuth.class);
      var actor = mock(UserModel.class);
      when(actor.getId()).thenReturn("actor");
      var client = mock(ClientModel.class);
      when(client.getId()).thenReturn("internal-client");
      when(auth.getRealm()).thenReturn(realm);
      when(auth.getUser()).thenReturn(actor);
      when(auth.getClient()).thenReturn(client);
      return new AdminEventBuilder(realm, auth, session, mock(ClientConnection.class));
    }

    @Override
    public void close() {
      captures.close();
      heads.close();
    }
  }
}
