package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FailurePolicyTest {
  @Test
  void defaultsRetainEveryFailureRegardlessOfDeliveryCountAndAge() {
    var policy = FailurePolicy.from(name -> null);
    assertEquals(FailurePolicy.Action.RETRY, policy.rejected("keycloak.events.a", Long.MAX_VALUE));
    assertFalse(policy.expired("keycloak.events.a", Instant.EPOCH, Instant.now()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"drop", "quarantine"})
  void removingMessagesRequiresAnExplicitScope(String action) {
    assertThrows(
        IllegalArgumentException.class,
        () -> FailurePolicy.from(Map.of("KND_CONSUMER_FAILURE_ACTION", action)::get));
  }

  @Test
  void rejectionThresholdAndScopeBothApply() {
    var policy =
        FailurePolicy.from(
            Map.of(
                    "KND_CONSUMER_FAILURE_ACTION",
                    "quarantine",
                    "KND_CONSUMER_FAILURE_SUBJECTS",
                    "keycloak.events.*.user.login",
                    "KND_CONSUMER_FAILURE_MIN_DELIVERIES",
                    "3")
                ::get);
    assertEquals(FailurePolicy.Action.RETRY, policy.rejected("keycloak.events.a.user.login", 2));
    assertEquals(
        FailurePolicy.Action.QUARANTINE, policy.rejected("keycloak.events.a.user.login", 3));
    assertEquals(FailurePolicy.Action.RETRY, policy.rejected("keycloak.events.b.admin.user", 30));
  }

  @Test
  void ageSheddingIsExplicitAndScopedIndependentlyOfRejections() {
    var policy =
        FailurePolicy.from(
            Map.of(
                    "KND_CONSUMER_FAILURE_ACTION",
                    "drop",
                    "KND_CONSUMER_FAILURE_SUBJECTS",
                    "keycloak.events.a.>",
                    "KND_CONSUMER_DROP_AFTER_SECONDS",
                    "60")
                ::get);
    Instant now = Instant.now();
    assertFalse(policy.expired("keycloak.events.a.user.login", now.minusSeconds(59), now));
    assertTrue(policy.expired("keycloak.events.a.user.login", now.minusSeconds(60), now));
    assertFalse(policy.expired("keycloak.events.b.user.login", Instant.EPOCH, now));
    assertFalse(policy.expired("keycloak.events.a.user.login", now.plusSeconds(60), now));
    assertThrows(
        IllegalArgumentException.class,
        () -> FailurePolicy.from(Map.of("KND_CONSUMER_DROP_AFTER_SECONDS", "60")::get));
  }
}
