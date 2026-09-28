package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PublicationPolicyTest {
  @Test
  void expiryWinsWhenBothLimitsAreReached() {
    var policy = new PublicationPolicy(1, 2);
    assertNull(policy.discardReason(1000, 1999, 1));
    assertEquals(DiscardReason.MAX_FAILURES, policy.discardReason(1000, 1999, 2));
    assertEquals(DiscardReason.EXPIRED, policy.discardReason(1000, 2000, 1));
    assertEquals(DiscardReason.EXPIRED, policy.discardReason(1000, 2000, 2));
  }

  @Test
  void limitsAreInclusiveAlternativesAndRetryNeverExpires() {
    var both = new PublicationPolicy(300, 20);
    assertFalse(both.shouldDiscard(1000, 300999, 19));
    assertTrue(both.shouldDiscard(1000, 301000, 0));
    assertTrue(both.shouldDiscard(1000, 1000, 20));
    assertTrue(both.shouldDiscard(1000, 301001, 21));
    assertTrue(new PublicationPolicy(null, 1).shouldDiscard(1000, 1000, 1));
    assertFalse(new PublicationPolicy(1, null).shouldDiscard(1000, 1999, Long.MAX_VALUE));
    assertFalse(PublicationPolicy.RETRY.shouldDiscard(0, Long.MAX_VALUE, Long.MAX_VALUE));
  }

  @Test
  void expiryArithmeticAndStorageBoundsCannotWrap() {
    assertThrows(
        ArithmeticException.class,
        () -> new PublicationPolicy(1, null).shouldDiscard(Long.MAX_VALUE, Long.MAX_VALUE, 0));
    for (int value : new int[] {0, -1, Integer.MAX_VALUE}) {
      assertThrows(IllegalArgumentException.class, () -> new PublicationPolicy(value, null));
      assertThrows(IllegalArgumentException.class, () -> new PublicationPolicy(null, value));
    }
    assertTrue(
        new PublicationPolicy(PublicationPolicy.MAX_AGE_SECONDS, PublicationPolicy.MAX_FAILURES)
            .shouldDiscard(0, 31_536_000_000L, 0));
  }
}
