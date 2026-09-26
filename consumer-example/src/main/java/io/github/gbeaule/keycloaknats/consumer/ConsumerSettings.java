package io.github.gbeaule.keycloaknats.consumer;

import io.nats.client.api.ConsumerConfiguration;
import java.time.Duration;
import java.util.function.Function;

/** Non-secret runtime settings shared by provisioning, workers and operator commands. */
public record ConsumerSettings(
    ProcessingLimits processing,
    FailurePolicy failures,
    int ackWaitMs,
    int ackTimeoutMs,
    int progressMs,
    int maxAckPending,
    int monitorSeconds,
    String healthBind,
    int healthPort,
    boolean autoMigrate) {

  private static final int MIN_ACK_WAIT_MS = 1_000;
  private static final int MAX_ACK_WAIT_MS = 3_600_000;
  private static final int MAX_ACK_TIMEOUT_MS = 60_000;
  private static final int MAX_PROGRESS_MS = 300_000;
  private static final int MAX_PENDING = 1_000_000;
  private static final int MAX_MONITOR_SECONDS = 3_600;
  private static final int MAX_PORT = 65_535;
  private static final long PROGRESS_SAFETY_FACTOR = 2;

  /** Bounded, positive waits are mandatory; zero only disables the optional HTTP endpoint. */
  public ConsumerSettings {
    final boolean invalidPolicies = processing == null || failures == null;
    final boolean invalidAckWait = ackWaitMs < MIN_ACK_WAIT_MS || ackWaitMs > MAX_ACK_WAIT_MS;
    final boolean invalidAckTimeout = ackTimeoutMs < 1 || ackTimeoutMs > MAX_ACK_TIMEOUT_MS;
    final boolean invalidProgress =
        progressMs < 1
            || progressMs > MAX_PROGRESS_MS
            || progressMs * PROGRESS_SAFETY_FACTOR >= ackWaitMs;
    final boolean invalidPending = maxAckPending < 1 || maxAckPending > MAX_PENDING;
    final boolean invalidMonitor = monitorSeconds < 1 || monitorSeconds > MAX_MONITOR_SECONDS;
    final boolean invalidBind = healthBind == null || healthBind.isBlank();
    final boolean invalidPort = healthPort < 0 || healthPort > MAX_PORT;
    if (invalidPolicies
        || invalidAckWait
        || invalidAckTimeout
        || invalidProgress
        || invalidPending
        || invalidMonitor
        || invalidBind
        || invalidPort) {
      throw new IllegalArgumentException("Invalid consumer settings or ACK progress interval");
    }
  }

  /** Reads the deployment environment; migrations are explicit by default. */
  public static ConsumerSettings from(Function<String, String> environment) {
    return new ConsumerConfig(environment).settings();
  }

  /** The live consumer policy, including backoff, determines the effective ACK deadline. */
  public void validateProgress(ConsumerConfiguration consumer) {
    Duration shortest = consumer.getAckWait();
    if (consumer.getBackoff() != null && !consumer.getBackoff().isEmpty()) {
      shortest = consumer.getBackoff().stream().min(Duration::compareTo).orElseThrow();
    }
    if (shortest == null
        || shortest.isZero()
        || shortest.isNegative()
        || Duration.ofMillis(progressMs * PROGRESS_SAFETY_FACTOR).compareTo(shortest) >= 0) {
      throw new IllegalStateException("Progress interval must be less than half the live AckWait");
    }
  }
}
