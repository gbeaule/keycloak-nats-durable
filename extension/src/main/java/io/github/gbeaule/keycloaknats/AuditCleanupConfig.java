package io.github.gbeaule.keycloaknats;

import java.time.Duration;

/** Node maintenance settings; independent of captured publication policies. */
public record AuditCleanupConfig(
    Duration retention, Duration interval, int batchSize, int maxBatches, int timeoutSeconds) {
  /** Bounds storage age, scheduler frequency and work per sweep. */
  public AuditCleanupConfig {
    if (retention == null
        || retention.isNegative()
        || retention.compareTo(Duration.ofDays(3650)) > 0) {
      throw new IllegalArgumentException("audit-retention-seconds must be 0..315360000");
    }
    if (interval == null
        || interval.compareTo(Duration.ofSeconds(1)) < 0
        || interval.compareTo(Duration.ofHours(1)) > 0) {
      throw new IllegalArgumentException("audit-cleanup-interval-ms must be 1000..3600000");
    }
    if (batchSize < 1 || batchSize > 500) {
      throw new IllegalArgumentException("audit-cleanup-batch-size must be 1..500");
    }
    if (maxBatches < 1 || maxBatches > 100) {
      throw new IllegalArgumentException("audit-cleanup-max-batches must be 1..100");
    }
    if (timeoutSeconds < 1 || timeoutSeconds > 300) {
      throw new IllegalArgumentException("audit-cleanup-timeout-seconds must be 1..300");
    }
  }
}
