package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class OutboxEventTest {
  @Test
  void exhaustedAttemptCounterSaturatesButRetrySchedulingContinues() throws Exception {
    var row = new OutboxEvent("id", "subject", "{}", 0);
    // Simulate persisted state near the limit, rather than performing a lifetime of retries.
    var attempts = OutboxEvent.class.getDeclaredField("attempts");
    attempts.setAccessible(true);
    attempts.setLong(row, Long.MAX_VALUE - 1);
    var config = BridgeConfig.from(Map.of());
    for (int i = 0; i < 3; i++) {
      long delay = RetryBackoff.delay(config, row.attempts());
      assertTrue(delay >= config.retryMax().toMillis() / 2);
      assertTrue(delay <= config.retryMax().toMillis());
      row.failed(1000 + i, "IOException");
      assertEquals(Long.MAX_VALUE, row.attempts());
      assertEquals(1000 + i, row.nextAttemptAt());
      assertEquals("IOException", row.lastError());
      assertEquals("id", row.id());
    }
  }
}
