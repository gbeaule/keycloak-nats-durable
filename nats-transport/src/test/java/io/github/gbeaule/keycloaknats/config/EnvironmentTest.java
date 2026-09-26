package io.github.gbeaule.keycloaknats.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Objects;
import org.junit.jupiter.api.Test;

class EnvironmentTest {
  @Test
  void processLookupPreservesPlatformCaseRules() {
    var environment = Environment.system();
    // A copied getenv() map loses Windows' case-insensitive System.getenv(name) lookup.
    for (String name : new String[] {"PATH", "Path", "pAtH"}) {
      assertTrue(
          Objects.equals(System.getenv(name), environment.optional(name)),
          "Environment lookup must preserve the platform's case rules");
    }
  }
}
