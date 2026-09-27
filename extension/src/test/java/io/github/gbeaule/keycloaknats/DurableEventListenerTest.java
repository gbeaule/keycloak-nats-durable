package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserProvider;
import org.mockito.ArgumentCaptor;

class DurableEventListenerTest {
  KeycloakSession session;
  EntityManager em;
  KeycloakTransactionManager tx;
  DurableEventListener listener;
  Runnable wakeRelay;

  @BeforeEach
  void setup() {
    session = mock(KeycloakSession.class);
    em = mock(EntityManager.class);
    tx = mock(KeycloakTransactionManager.class);
    when(session.getTransactionManager()).thenReturn(tx);
    when(tx.isActive()).thenReturn(true);
    var jpa = mock(JpaConnectionProvider.class);
    when(session.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
    when(jpa.getEntityManager()).thenReturn(em);
    wakeRelay = mock(Runnable.class);
    listener = new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay);
  }

  @Test
  void excludedEventsDoNotTouchDatabaseOrObserveUsers() throws Exception {
    var none = EventFilterTest.parse("{\"userEvents\":[],\"adminEvents\":[]}");
    listener =
        new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay, () -> none);
    listener.onEvent(EventEnvelopeTest.login());
    listener.onEvent(EventEnvelopeTest.admin(OperationType.UPDATE), false);
    verifyNoInteractions(session, em, tx, wakeRelay);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"realmIds\":[\"other-realm\"]",
        "\"subjects\":[\"other.>\"]",
        "\"clientIds\":[\"other-client\"]"
      })
  void independentScopeExclusionsAvoidPersistenceAndUserLookups(String scope) throws Exception {
    var policy =
        EventFilterTest.parse(
            "{"
                + scope
                + ",\"userEvents\":[\"*\"],\"adminEvents\":["
                + "{\"resourceType\":\"*\",\"operations\":[\"*\"]}]}");
    listener =
        new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay, () -> policy);
    listener.onEvent(EventEnvelopeTest.login());
    listener.onEvent(EventEnvelopeTest.admin(OperationType.UPDATE), false);
    verifyNoInteractions(session, em, tx, wakeRelay);
  }

  @Test
  void insertsAndFlushesBeforeEnlistingNotificationWithoutCommittingTheRequest() throws Exception {
    var event = EventEnvelopeTest.login();
    listener.onEvent(event);
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    var order = inOrder(tx, em);
    order.verify(tx).isActive();
    order.verify(em).persist(captured.capture());
    order.verify(em).flush();
    order.verify(tx).enlistAfterCompletion(any());
    verifyNoMoreInteractions(tx, em);
    verifyNoInteractions(wakeRelay);
    var json = new ObjectMapper().readTree(captured.getValue().payload());
    assertEquals(event.getId(), json.at("/data/keycloakEventId").textValue());
    assertEquals(event.getUserId(), json.at("/data/userId").textValue());
    assertEquals(captured.getValue().id(), json.get("id").textValue());
  }

  @Test
  void persistenceFailureExplicitlyMarksRollbackOnly() {
    var failure = new IllegalStateException("disk-full");
    doThrow(failure).when(em).persist(any());
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login())));
    verify(tx).setRollbackOnly();
    verify(em, never()).flush();
    verify(tx, never()).enlistAfterCompletion(any());
    verifyNoInteractions(wakeRelay);
  }

  @Test
  void serializationFailureAlsoMarksRollbackOnly() {
    var event = EventEnvelopeTest.login();
    event.setType(null);
    assertThrows(NullPointerException.class, () -> listener.onEvent(event));
    verify(tx).setRollbackOnly();
    verifyNoInteractions(em, wakeRelay);
    verify(tx, never()).enlistAfterCompletion(any());
  }

  @Test
  void flushFailureIsCaughtBeforeLeavingTheListener() {
    doThrow(new IllegalStateException("constraint failure")).when(em).flush();
    assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
    verify(tx).setRollbackOnly();
    verify(tx, never()).enlistAfterCompletion(any());
    verifyNoInteractions(wakeRelay);
  }

  @Test
  void inactiveTransactionFailsClosed() {
    when(tx.isActive()).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
    verify(em, never()).persist(any());
    verify(tx).setRollbackOnly();
    verifyNoInteractions(em, wakeRelay);
    verify(tx, never()).enlistAfterCompletion(any());
  }

  @Test
  void deletingDoesNotReadDeletedUser() {
    listener.onEvent(EventEnvelopeTest.admin(OperationType.DELETE), false);
    verify(session, never()).users();
    verify(em).persist(any());
  }

  @Test
  void actionDoesNotClaimAnEnablementObservation() {
    listener.onEvent(EventEnvelopeTest.admin(OperationType.ACTION), false);
    verify(session, never()).users();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    assertFalse(captured.getValue().payload().contains("userEnabled"));
  }

  @Test
  void coalescesEventsAndWakesOnlyAfterSuccessfulCommit() {
    listener.onEvent(EventEnvelopeTest.login());
    listener.onEvent(EventEnvelopeTest.login());
    verifyNoInteractions(wakeRelay);
    var callback = ArgumentCaptor.forClass(KeycloakTransaction.class);
    verify(tx).enlistAfterCompletion(callback.capture());
    callback.getValue().begin();
    callback.getValue().commit();
    verify(wakeRelay).run();
  }

  @Test
  void rolledBackTransactionDoesNotWakeRelay() {
    listener.onEvent(EventEnvelopeTest.login());
    var callback = ArgumentCaptor.forClass(KeycloakTransaction.class);
    verify(tx).enlistAfterCompletion(callback.capture());
    callback.getValue().begin();
    callback.getValue().rollback();
    verifyNoInteractions(wakeRelay);
  }

  @Test
  void fatalCaptureFailuresAlsoMarkRollbackAndNeverNotify() {
    var failure = new AssertionError("fatal persistence failure");
    doThrow(failure).when(em).persist(any());
    assertSame(
        failure,
        assertThrows(AssertionError.class, () -> listener.onEvent(EventEnvelopeTest.login())));
    verify(tx).setRollbackOnly();
    verify(tx, never()).enlistAfterCompletion(any());
    verify(em, never()).flush();
    verifyNoInteractions(wakeRelay);
  }

  @Test
  void statePredicateRejectsEnabledAccountsAfterLookupWithoutEnteringTheOutbox() throws Exception {
    final var policy =
        EventFilterTest.parse(
            """
            {"userEvents":[],"adminEvents":[
              {"resourceType":"USER","operations":["UPDATE"],"userEnabled":false}]}
            """);
    var realms = mock(RealmProvider.class);
    var users = mock(UserProvider.class);
    var realm = mock(RealmModel.class);
    var user = mock(UserModel.class);
    when(session.realms()).thenReturn(realms);
    when(session.users()).thenReturn(users);
    when(realms.getRealm("realm")).thenReturn(realm);
    when(users.getUserById(realm, "target")).thenReturn(user);
    when(user.isEnabled()).thenReturn(true);
    listener =
        new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay, () -> policy);
    listener.onEvent(EventEnvelopeTest.admin(OperationType.UPDATE), true);
    verify(user).isEnabled();
    verifyNoInteractions(em, tx, wakeRelay);
    verify(session, never()).getProvider(JpaConnectionProvider.class);
  }

  @Test
  void notificationEnlistmentFailureMarksTheRequestForRollback() {
    var failure = new IllegalStateException("completion enlistment failed");
    doThrow(failure).when(tx).enlistAfterCompletion(any());
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login())));
    verify(em).persist(any(OutboxEvent.class));
    verify(em).flush();
    verify(tx).setRollbackOnly();
    verifyNoInteractions(wakeRelay);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void recordsObservedEnablementForDirectSuccessfulUserUpdates(boolean enabled) throws Exception {
    var realms = mock(RealmProvider.class);
    var users = mock(UserProvider.class);
    var realm = mock(RealmModel.class);
    var user = mock(UserModel.class);
    when(session.realms()).thenReturn(realms);
    when(session.users()).thenReturn(users);
    when(realms.getRealm("realm")).thenReturn(realm);
    when(users.getUserById(realm, "target")).thenReturn(user);
    when(user.isEnabled()).thenReturn(enabled);
    var event = EventEnvelopeTest.admin(OperationType.UPDATE);
    listener.onEvent(event, true);
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    var data = new ObjectMapper().readTree(captured.getValue().payload()).get("data");
    assertEquals(Boolean.toString(enabled), data.get("userEnabled").toString());
    assertEquals("target", data.get("userId").textValue());
    assertFalse(data.has("representation"));
    assertFalse(captured.getValue().payload().contains("sensitive-password"));
    verify(users).getUserById(realm, "target");
    verify(tx, never()).setRollbackOnly();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void missingRealmOrUserDoesNotInventAnEnablementState(boolean realmExists) throws Exception {
    var realms = mock(RealmProvider.class);
    var users = mock(UserProvider.class);
    when(session.realms()).thenReturn(realms);
    if (realmExists) {
      when(realms.getRealm("realm")).thenReturn(mock(RealmModel.class));
      when(session.users()).thenReturn(users);
    }
    listener.onEvent(EventEnvelopeTest.admin(OperationType.CREATE), false);
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    var data = new ObjectMapper().readTree(captured.getValue().payload()).get("data");
    assertEquals("target", data.get("userId").textValue());
    assertFalse(data.has("userEnabled"));
    if (!realmExists) {
      verifyNoInteractions(users);
      verify(session, never()).users();
    }
  }

  @Test
  void failedOrNestedAdminOperationsNeverObserveCurrentAccountState() throws Exception {
    var failed = EventEnvelopeTest.admin(OperationType.UPDATE);
    failed.setError("forbidden");
    listener.onEvent(failed, false);
    var nested = EventEnvelopeTest.admin(OperationType.UPDATE);
    nested.setResourcePath("users/target/role-mappings");
    listener.onEvent(nested, false);
    verify(session, never()).realms();
    verify(session, never()).users();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em, times(2)).persist(captured.capture());
    for (OutboxEvent event : captured.getAllValues()) {
      assertFalse(new ObjectMapper().readTree(event.payload()).get("data").has("userEnabled"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void completionResetsNotificationCoalescingForTheNextTransaction(boolean commit) {
    listener.onEvent(EventEnvelopeTest.login());
    var callback = ArgumentCaptor.forClass(KeycloakTransaction.class);
    verify(tx).enlistAfterCompletion(callback.capture());
    callback.getValue().begin();
    if (commit) {
      callback.getValue().commit();
    } else {
      callback.getValue().rollback();
    }
    listener.onEvent(EventEnvelopeTest.login());
    verify(tx, times(2)).enlistAfterCompletion(any());
    verify(wakeRelay, times(commit ? 1 : 0)).run();
    listener.close();
    verify(tx, never()).commit();
    verify(em, never()).close();
  }
}
