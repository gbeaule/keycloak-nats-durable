package io.github.gbeaule.keycloaknats;

/** Capture-time limits; both absent means retry indefinitely. Never serialized on the wire. */
record PublicationPolicy(Integer maxAgeSeconds, Integer maxFailures) {
  static final int MAX_AGE_SECONDS = 365 * 24 * 60 * 60;
  static final int MAX_FAILURES = 1_000_000;
  static final PublicationPolicy RETRY = new PublicationPolicy(null, null);

  PublicationPolicy {
    checkLimit(maxAgeSeconds, MAX_AGE_SECONDS);
    checkLimit(maxFailures, MAX_FAILURES);
  }

  /** Times are database epoch milliseconds; failures count completed, failed original publishes. */
  boolean shouldDiscard(long capturedAt, long databaseNow, long failures) {
    return discardReason(capturedAt, databaseNow, failures) != null;
  }

  DiscardReason discardReason(long capturedAt, long databaseNow, long failures) {
    if (maxAgeSeconds != null
        && databaseNow >= Math.addExact(capturedAt, maxAgeSeconds.longValue() * 1000)) {
      return DiscardReason.EXPIRED;
    }
    return maxFailures != null && failures >= maxFailures ? DiscardReason.MAX_FAILURES : null;
  }

  private static void checkLimit(Integer value, int maximum) {
    if (value != null && (value <= 0 || value > maximum)) {
      throw new IllegalArgumentException("Publication limit outside supported range");
    }
  }
}
