package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
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
    verifyNoInteractions(em, tx, wakeRelay);
    verify(session, never()).users();
  }

  @Test
  void insertsIntoRequestTransactionWithoutNetworkOrCommit() {
    listener.onEvent(EventEnvelopeTest.login());
    verify(em).persist(any(OutboxEvent.class));
    verify(tx, never()).commit();
    verify(tx, never()).setRollbackOnly();
  }

  @Test
  void persistenceFailureExplicitlyMarksRollbackOnly() {
    doThrow(new IllegalStateException("disk-full")).when(em).persist(any());
    assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
    verify(tx).setRollbackOnly();
  }

  @Test
  void serializationFailureAlsoMarksRollbackOnly() {
    var event = EventEnvelopeTest.login();
    event.setType(null);
    assertThrows(NullPointerException.class, () -> listener.onEvent(event));
    verify(tx).setRollbackOnly();
  }

  @Test
  void flushFailureIsCaughtBeforeLeavingTheListener() {
    doThrow(new IllegalStateException("constraint failure")).when(em).flush();
    assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
    verify(tx).setRollbackOnly();
  }

  @Test
  void inactiveTransactionFailsClosed() {
    when(tx.isActive()).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
    verify(em, never()).persist(any());
    verify(tx).setRollbackOnly();
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
}
