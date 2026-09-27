package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NatsDiagnosticsTest {
  @Test
  void retainsCauseCategoriesWithoutLeakingExceptionText() {
    var failure =
        new IOException(
            "nats://user:secret@host", new ConnectException("token=secret\ninjected log"));
    String diagnostic = NatsDiagnostics.describe(failure);
    assertEquals("IOException causedBy=ConnectException", diagnostic);
    assertFalse(diagnostic.contains("secret"));
    assertFalse(diagnostic.contains("\n"));
  }

  @Test
  void reportsWhichStreamRuleFailed() {
    assertTrue(
        NatsDiagnostics.describe(new UnsafeStreamException("DiscardNew is required"))
            .contains("DiscardNew is required"));
  }

  @Test
  void categorizesRemoteErrorsWithoutEchoingCredentialsOrSubjects() {
    assertEquals(
        "permission_denied",
        NatsDiagnostics.serverErrorCategory(
            "Permissions Violation for Publish to customer-secret"));
    assertEquals(
        "authentication_rejected",
        NatsDiagnostics.serverErrorCategory("Authorization Violation token=secret"));
    assertFalse(NatsDiagnostics.serverErrorCategory("secret\nanything").contains("secret"));
  }

  @ParameterizedTest
  @CsvSource({
    "AUTHENTICATION failed secret,authentication_rejected",
    "maximum payload secret,payload_limit",
    "Stale connection secret,stale_connection",
    "unrecognized secret,server_error (inspect the NATS server logs)"
  })
  void categorizesOtherServerFailuresWithoutEchoingUntrustedText(String input, String expected) {
    assertEquals(expected, NatsDiagnostics.serverErrorCategory(input));
  }

  @Test
  void absentRemoteErrorsAndAbsentCausesAreHandledWithoutInventingDetails() {
    assertEquals(
        "server_error (inspect the NATS server logs)", NatsDiagnostics.serverErrorCategory(null));
    assertEquals("", NatsDiagnostics.describe(null));
  }

  @Test
  void causeDepthIsBoundedEvenForCyclicExceptionGraphs() {
    var first = new IOException("secret-one");
    var second = new IllegalStateException("secret-two", first);
    first.initCause(second);
    assertEquals(
        "IOException causedBy=IllegalStateException"
            + " causedBy=IOException causedBy=IllegalStateException",
        NatsDiagnostics.describe(first));
  }

  @Test
  void jetStreamErrorsIncludeOnlyNumericCodesNotRemoteDescriptions() {
    var failure = mock(JetStreamApiException.class);
    when(failure.getErrorCode()).thenReturn(503);
    when(failure.getApiErrorCode()).thenReturn(10008);
    when(failure.getMessage()).thenReturn("private-stream and secret-token");
    assertEquals(
        "JetStreamApiException status=503 apiCode=10008", NatsDiagnostics.describe(failure));
  }

  @Test
  void listenerCallbacksLogSafeContextWithoutInspectingConnectionOrMessageContents() {
    Logger logger = Logger.getLogger(NatsDiagnostics.class.getName());
    final Level previous = logger.getLevel();
    final boolean parents = logger.getUseParentHandlers();
    List<LogRecord> records = new ArrayList<>();
    var handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            records.add(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(handler);
    logger.setLevel(Level.ALL);
    logger.setUseParentHandlers(false);
    var connection = mock(Connection.class);
    var consumer = mock(Consumer.class);
    var message = mock(Message.class);
    try {
      var listener = new NatsDiagnostics();
      listener.connectionEvent(connection, ConnectionListener.Events.DISCONNECTED);
      listener.connectionEvent(connection, ConnectionListener.Events.CONNECTED);
      listener.errorOccurred(connection, "Maximum payload secret-token");
      listener.exceptionOccurred(connection, new IOException("nats://user:secret-token@host"));
      listener.slowConsumerDetected(connection, consumer);
      listener.messageDiscarded(connection, message);
      listener.socketWriteTimeout(connection);
      assertEquals(7, records.size());
      var formatter = new SimpleFormatter();
      for (LogRecord record : records) {
        assertFalse(formatter.format(record).contains("secret-token"));
        assertNull(record.getThrown(), "Throwable text would leak remote details");
      }
      assertTrue(formatter.format(records.get(2)).contains("reason=payload_limit"));
      assertTrue(formatter.format(records.get(3)).contains("NATS client exception; IOException"));
      verifyNoInteractions(connection, consumer, message);
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previous);
      logger.setUseParentHandlers(parents);
    }
  }
}
