package io.github.gbeaule.keycloaknats.config;

import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

/**
 * Shared environment boundary and secret-safe numeric parsing for the provider and example tools.
 */
public final class Environment {
  private final Function<String, String> values;

  /** An injectable source keeps configuration independent of the process environment in tests. */
  public Environment(Function<String, String> values) {
    this.values = Objects.requireNonNull(values);
  }

  /** Preserves native lookup semantics, including case-insensitive variable names on Windows. */
  public static Environment system() {
    return new Environment(System::getenv);
  }

  /** Returns the exact value, preserving whitespace in secrets, or null when absent. */
  public String optional(String name) {
    return values.apply(name);
  }

  /** Uses the default only when the variable is absent. */
  public String value(String name, String fallback) {
    String configured = optional(name);
    return configured == null ? fallback : configured;
  }

  /** Parses an integer without echoing the supplied value in an exception. */
  public int integer(String name, int fallback) {
    try {
      return Integer.parseInt(value(name, Integer.toString(fallback)));
    } catch (NumberFormatException invalid) {
      throw new IllegalArgumentException("Invalid integer setting " + name);
    }
  }

  /** Applies a deployment setting's inclusive numeric range. */
  public int integer(String name, int fallback, int minimum, int maximum) {
    int parsed = integer(name, fallback);
    if (parsed < minimum || parsed > maximum) {
      throw new IllegalArgumentException("Out-of-range setting " + name);
    }
    return parsed;
  }

  /** Parses a long without echoing the supplied value in an exception. */
  public long longValue(String name, long fallback) {
    try {
      return Long.parseLong(value(name, Long.toString(fallback)));
    } catch (NumberFormatException invalid) {
      throw new IllegalArgumentException("Invalid long setting " + name);
    }
  }

  /** Maps a provider key such as {@code poll-ms} to {@code KND_POLL_MS}. */
  public static String name(String key) {
    return "KND_" + key.replace('-', '_').toUpperCase(Locale.ROOT);
  }
}
