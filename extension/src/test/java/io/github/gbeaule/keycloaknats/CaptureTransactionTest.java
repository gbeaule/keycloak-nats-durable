package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.services.DefaultKeycloakTransactionManager;
import org.keycloak.tracing.TracingProvider;

class CaptureTransactionTest {
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
}
