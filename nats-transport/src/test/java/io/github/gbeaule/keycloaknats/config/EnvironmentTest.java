package io.github.gbeaule.keycloaknats.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EnvironmentTest {
  @Test
  void processLookupPreservesPlatformCaseRules() {
    var environment = Environment.system();
    // A copied getenv() map loses Windows' case-insensitive System.getenv(name) lookup.
    for (String name : new String[] {"PATH", "Path", "pAtH"}) {
      assertEquals(
          System.getenv(name),
          environment.optional(name),
          "Environment lookup must preserve the platform's case rules");
    }
  }

  @Test
  void defaultsApplyOnlyToAbsentValuesAndSecretsArePreservedExactly() {
    var environment = new Environment(Map.of("EMPTY", "", "SECRET", " token \n")::get);
    assertNull(environment.optional("MISSING"));
    assertEquals("fallback", environment.value("MISSING", "fallback"));
    assertEquals("", environment.value("EMPTY", "fallback"));
    assertEquals(" token \n", environment.optional("SECRET"));
    assertEquals(" token \n", environment.value("SECRET", "fallback"));
    assertEquals(27, environment.integer("MISSING", 27));
    assertEquals(Long.MAX_VALUE, environment.longValue("MISSING", Long.MAX_VALUE));
  }

  @Test
  void parsesTheFullIntegerAndLongRanges() {
    var environment =
        new Environment(
            Map.of(
                    "INT_MIN",
                    "-2147483648",
                    "INT_MAX",
                    "2147483647",
                    "LONG_MIN",
                    "-9223372036854775808",
                    "LONG_MAX",
                    "9223372036854775807")
                ::get);
    assertEquals(Integer.MIN_VALUE, environment.integer("INT_MIN", 0));
    assertEquals(Integer.MAX_VALUE, environment.integer("INT_MAX", 0));
    assertEquals(Long.MIN_VALUE, environment.longValue("LONG_MIN", 0));
    assertEquals(Long.MAX_VALUE, environment.longValue("LONG_MAX", 0));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " 1", "1 ", "1.0", "secret-token", "2147483648", "-2147483649"})
  void malformedIntegersNeverFallBackOrExposeTheInput(String value) {
    var environment = new Environment(name -> value);
    var failure =
        assertThrows(IllegalArgumentException.class, () -> environment.integer("PORT", 42));
    assertEquals("Invalid integer setting PORT", failure.getMessage());
    assertNull(failure.getCause(), "A parser cause could expose the configured value");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " 1",
        "1 ",
        "1.0",
        "secret-token",
        "9223372036854775808",
        "-9223372036854775809"
      })
  void malformedLongsNeverFallBackOrExposeTheInput(String value) {
    var environment = new Environment(name -> value);
    var failure =
        assertThrows(IllegalArgumentException.class, () -> environment.longValue("SIZE", 42));
    assertEquals("Invalid long setting SIZE", failure.getMessage());
    assertNull(failure.getCause(), "A parser cause could expose the configured value");
  }

  @Test
  void numericBoundsAreInclusiveAndAlsoApplyToDefaults() {
    var environment =
        new Environment(Map.of("MIN", "1", "MAX", "16", "LOW", "0", "HIGH", "17")::get);
    assertEquals(1, environment.integer("MIN", 8, 1, 16));
    assertEquals(16, environment.integer("MAX", 8, 1, 16));
    assertEquals(8, environment.integer("MISSING", 8, 1, 16));
    for (String name : new String[] {"LOW", "HIGH", "MISSING"}) {
      var failure =
          assertThrows(IllegalArgumentException.class, () -> environment.integer(name, 17, 1, 16));
      assertEquals("Out-of-range setting " + name, failure.getMessage());
      assertNull(failure.getCause());
    }
  }

  @Test
  void providerNamesAreIndependentOfTheDefaultLocale() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      assertEquals("KND_RETRY_INITIAL_MS", Environment.name("retry-initial-ms"));
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void lookupSourceIsRequired() {
    assertThrows(NullPointerException.class, () -> new Environment(null));
  }
}
