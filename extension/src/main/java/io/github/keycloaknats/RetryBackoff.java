package io.github.keycloaknats;

import java.util.concurrent.ThreadLocalRandom;

public final class RetryBackoff {
  private RetryBackoff() {}

  /**
   * Equal jitter avoids synchronized retry storms; arithmetic is bounded even after years of
   * failure.
   */
  public static long delay(BridgeConfig config, long previousAttempts) {
    long ceiling = config.retryInitial().toMillis();
    for (int i = 0;
        i < Math.min(63, Math.max(0, previousAttempts)) && ceiling < config.retryMax().toMillis();
        i++) ceiling = Math.min(config.retryMax().toMillis(), ceiling * 2);
    long floor = Math.max(1, ceiling / 2);
    return ThreadLocalRandom.current().nextLong(floor, ceiling + 1);
  }
}
