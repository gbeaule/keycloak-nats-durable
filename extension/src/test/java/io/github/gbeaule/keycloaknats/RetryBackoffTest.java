package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RetryBackoffTest {
  @ParameterizedTest
  @CsvSource({"-1,1000", "0,1000", "1,2000", "5,32000", "6,60000", "9223372036854775807,60000"})
  void exponentialCeilingCapsWithoutOverflow(long attempts, long expected) {
    assertEquals(expected, RetryBackoff.ceiling(BridgeConfig.from(Map.of()), attempts));
  }

  @Test
  void jitterRemainsPositiveAtTheMinimumAndInsideItsUpperHalf() {
    var minimum = BridgeConfig.from(Map.of("retry-initial-ms", "1", "retry-max-ms", "1"));
    assertEquals(1, RetryBackoff.sampleDelay(minimum, Long.MAX_VALUE));
    var capped = BridgeConfig.from(Map.of("retry-initial-ms", "3", "retry-max-ms", "7"));
    for (long attempts : new long[] {0, 1, 2, Long.MAX_VALUE}) {
      long ceiling = RetryBackoff.ceiling(capped, attempts);
      long delay = RetryBackoff.sampleDelay(capped, attempts);
      assertTrue(delay >= Math.max(1, ceiling / 2) && delay <= ceiling);
    }
  }
}
