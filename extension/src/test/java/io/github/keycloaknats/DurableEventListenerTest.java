package io.github.keycloaknats;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import jakarta.persistence.EntityManager;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransactionManager;

class DurableEventListenerTest {
  KeycloakSession session;
  EntityManager em;
  KeycloakTransactionManager tx;
  DurableEventListener listener;

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
    listener = new DurableEventListener(session, BridgeConfig.from(Map.of()));
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
}
