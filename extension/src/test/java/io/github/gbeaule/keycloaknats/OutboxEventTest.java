package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.hibernate.internal.util.ReflectHelper;
import org.hibernate.property.access.internal.PropertyAccessStrategyFieldImpl;
import org.junit.jupiter.api.Test;

class OutboxEventTest {
  @Test
  void hibernateFieldHydrationRestoresRetryStateWithoutRecreatingTheEvent() throws Exception {
    var row = ReflectHelper.getDefaultConstructor(OutboxEvent.class).newInstance();
    Map<String, Object> persisted =
        Map.of(
            "id",
            "persisted-id",
            "subject",
            "original.subject",
            "payload",
            "{\"value\":1}",
            "createdAt",
            123L,
            "nextAttemptAt",
            456L,
            "attempts",
            7L,
            "lastError",
            "IOException");
    persisted.forEach(
        (field, value) ->
            PropertyAccessStrategyFieldImpl.INSTANCE
                .buildPropertyAccess(OutboxEvent.class, field, true)
                .getSetter()
                .set(row, value));
    assertEquals("persisted-id", row.id());
    assertEquals("original.subject", row.subject());
    assertEquals("{\"value\":1}", row.payload());
    assertEquals(123, row.createdAt());
    assertEquals(456, row.nextAttemptAt());
    assertEquals(7, row.attempts());
    assertEquals("IOException", row.lastError());
    row.failed(789, "TimeoutException");
    assertEquals(8, row.attempts());
    assertEquals(789, row.nextAttemptAt());
    assertEquals("TimeoutException", row.lastError());
    assertEquals("persisted-id", row.id());
    assertEquals("original.subject", row.subject());
    assertEquals("{\"value\":1}", row.payload());
    assertEquals(123, row.createdAt());
  }

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
