package io.github.gbeaule.keycloaknats.consumer;

import io.github.gbeaule.keycloaknats.routing.SubjectPattern;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;

/** Explicit, subject-scoped shedding. Infrastructure failures are never eligible. */
public record FailurePolicy(
    Action action, int minDeliveries, List<SubjectPattern> subjects, int maxAgeSeconds) {
  private static final int MIN_DELIVERIES = 1;
  private static final int MAX_DELIVERIES = 1_000_000;
  private static final int AGE_SHEDDING_DISABLED = 0;
  private static final int MAX_AGE_SECONDS = (int) Duration.ofDays(365).toSeconds();

  /** Retry preserves the broker copy; the other actions require a committed audit record first. */
  public enum Action {
    RETRY,
    QUARANTINE,
    DROP
  }

  /** Requires an explicit subject scope whenever a policy can remove a broker message. */
  public FailurePolicy {
    subjects = List.copyOf(subjects);
    final boolean invalidAction = action == null;
    final boolean invalidDeliveries =
        minDeliveries < MIN_DELIVERIES || minDeliveries > MAX_DELIVERIES;
    final boolean invalidAge =
        maxAgeSeconds < AGE_SHEDDING_DISABLED || maxAgeSeconds > MAX_AGE_SECONDS;
    final boolean missingSheddingScope = action != Action.RETRY && subjects.isEmpty();
    final boolean ageWithoutDrop = maxAgeSeconds > AGE_SHEDDING_DISABLED && action != Action.DROP;
    if (invalidAction
        || invalidDeliveries
        || invalidAge
        || missingSheddingScope
        || ageWithoutDrop) {
      throw new IllegalArgumentException(
          "Invalid failure policy; shedding requires explicit subjects");
    }
  }

  /** No messages are dropped or quarantined with the default configuration. */
  public static FailurePolicy from(Function<String, String> environment) {
    return new ConsumerConfig(environment).failures();
  }

  /**
   * Delivery counts include crashes and lost ACKs; they are not a count of application failures.
   */
  public Action rejected(String subject, long deliveries) {
    return deliveries >= minDeliveries && includes(subject) ? action : Action.RETRY;
  }

  /**
   * Optional age shedding uses the broker's original publication time, not untrusted event time.
   */
  public boolean expired(String subject, Instant published, Instant now) {
    return action == Action.DROP
        && maxAgeSeconds > AGE_SHEDDING_DISABLED
        && includes(subject)
        && Duration.between(published, now).compareTo(Duration.ofSeconds(maxAgeSeconds)) >= 0;
  }

  private boolean includes(String subject) {
    return subjects.stream().anyMatch(pattern -> pattern.matches(subject));
  }
}
