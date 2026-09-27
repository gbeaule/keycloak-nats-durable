package io.github.gbeaule.keycloaknats;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/** Descriptive per-user position. Missing positions may have been discarded locally. */
record EventOrdering(String realmId, String userId, long sequence) {
  EventOrdering {
    encode(realmId);
    encode(userId);
    if (sequence <= 0) {
      throw new IllegalArgumentException("Ordering sequence must be positive");
    }
  }

  String key() {
    return "u." + encode(realmId) + "." + encode(userId);
  }

  Map<String, String> wireValue() {
    return Map.of("key", key(), "sequence", Long.toString(sequence));
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
    if (!new String(bytes, StandardCharsets.UTF_8).equals(value)) {
      throw new IllegalArgumentException("Ordering identity must be valid UTF-8");
    }
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }
}
