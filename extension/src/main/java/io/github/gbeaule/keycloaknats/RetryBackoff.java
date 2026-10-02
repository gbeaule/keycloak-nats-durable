package io.github.gbeaule.keycloaknats;

import java.util.concurrent.ThreadLocalRandom;

/** Bounded exponential retry delays with jitter to spread recovery work across nodes. */
public final class RetryBackoff {
  private RetryBackoff() {}

  /**
   * Equal jitter avoids synchronized retry storms; arithmetic is bounded even after years of
   * failure.
   */
  public static long sampleDelay(BridgeConfig config, long previousAttempts) {
    long ceiling = ceiling(config, previousAttempts);
    return ThreadLocalRandom.current().nextLong(Math.max(1, ceiling / 2), ceiling + 1);
  }

  static long ceiling(BridgeConfig config, long previousAttempts) {
    long ceiling = config.retryInitial().toMillis();
    for (int i = 0;
        i < Math.min(Long.SIZE - 1, Math.max(0, previousAttempts))
            && ceiling < config.retryMax().toMillis();
        i++) {
      ceiling = Math.min(config.retryMax().toMillis(), ceiling * 2);
    }
    return ceiling;
  }
}
