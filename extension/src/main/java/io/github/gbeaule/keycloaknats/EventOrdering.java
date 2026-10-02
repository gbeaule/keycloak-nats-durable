package io.github.gbeaule.keycloaknats;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/** Descriptive per-user position. Missing positions may have been discarded locally. */
record EventOrdering(String realmId, String userId, long sequence) {
  EventOrdering {
    keyFor(realmId, userId);
    if (sequence <= 0) {
      throw new IllegalArgumentException("Ordering sequence must be positive");
    }
  }

  String key() {
    return keyFor(realmId, userId);
  }

  Map<String, String> wireValue() {
    return Map.of("key", key(), "sequence", Long.toString(sequence));
  }

  static String keyFor(String realmId, String userId) {
    String key = "u." + encode(realmId) + "." + encode(userId);
    if (key.length() > 2048) {
      throw new IllegalArgumentException("Ordering key exceeds storage limit");
    }
    return key;
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
