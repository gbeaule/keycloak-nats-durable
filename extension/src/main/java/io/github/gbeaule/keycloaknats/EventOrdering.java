package io.github.gbeaule.keycloaknats;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/** Descriptive per-user position. Missing positions may have been discarded locally. */
final class EventOrdering {
  private final String realmId;
  private final String userId;
  private final long sequence;
  private final String key;

  EventOrdering(String realmId, String userId, long sequence) {
    this.key = "u." + encode(realmId) + "." + encode(userId);
    if (key.length() > 2048) {
      throw new IllegalArgumentException("Ordering key exceeds storage limit");
    }
    if (sequence <= 0) {
      throw new IllegalArgumentException("Ordering sequence must be positive");
    }
    this.realmId = realmId;
    this.userId = userId;
    this.sequence = sequence;
  }

  String realmId() {
    return realmId;
  }

  String userId() {
    return userId;
  }

  long sequence() {
    return sequence;
  }

  String key() {
    return key;
  }

  Map<String, String> wireValue() {
    return Map.of("key", key, "sequence", Long.toString(sequence));
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof EventOrdering ordering
        && sequence == ordering.sequence
        && key.equals(ordering.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, sequence);
  }

  static long parseSequence(String value) {
    if (value == null || !value.matches("[1-9][0-9]*")) {
      throw new IllegalArgumentException("Ordering sequence must be a positive decimal string");
    }
    return Long.parseLong(value);
  }

  private static String encode(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Ordering requires a realm and affected user");
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (value.codePointCount(0, value.length()) > 255) {
      throw new IllegalArgumentException("Ordering identity exceeds storage limit");
    }
    if (!new String(bytes, StandardCharsets.UTF_8).equals(value)) {
      throw new IllegalArgumentException("Ordering identity must be valid UTF-8");
    }
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }
}
