package io.github.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import org.junit.jupiter.api.Test;

class NatsDiagnosticsTest {
  @Test
  void retainsCauseCategoriesWithoutLeakingExceptionText() {
    var failure =
        new IOException(
            "nats://user:secret@host", new ConnectException("token=secret\ninjected log"));
    String diagnostic = NatsDiagnostics.describe(failure);
    assertTrue(diagnostic.contains("IOException"));
    assertTrue(diagnostic.contains("ConnectException"));
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
}
