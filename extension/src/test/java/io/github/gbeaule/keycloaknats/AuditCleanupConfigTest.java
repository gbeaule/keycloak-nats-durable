package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class AuditCleanupConfigTest {
  @Test
  void defaultsBoundEachTransactionAndSweepIndependentlyOfDelivery() {
    assertEquals(
        new AuditCleanupConfig(Duration.ofDays(7), Duration.ofMinutes(1), 500, 10, 10),
        BridgeConfig.from(Map.of()).auditCleanup());
  }

  @ParameterizedTest
  @CsvSource({
    "audit-retention-seconds,-1", "audit-retention-seconds,315360001",
    "audit-cleanup-interval-ms,999", "audit-cleanup-interval-ms,3600001",
    "audit-cleanup-batch-size,0", "audit-cleanup-batch-size,501",
    "audit-cleanup-max-batches,0", "audit-cleanup-max-batches,101",
    "audit-cleanup-timeout-seconds,0", "audit-cleanup-timeout-seconds,301"
  })
  void rejectsUnboundedMaintenance(String key, String value) {
    assertThrows(IllegalArgumentException.class, () -> BridgeConfig.from(Map.of(key, value)));
  }

  @Test
  void includesSettingEndpoints() {
    assertEquals(
        new AuditCleanupConfig(Duration.ZERO, Duration.ofSeconds(1), 1, 1, 1),
        BridgeConfig.from(
                Map.of(
                    "audit-retention-seconds",
                    "0",
                    "audit-cleanup-interval-ms",
                    "1000",
                    "audit-cleanup-batch-size",
                    "1",
                    "audit-cleanup-max-batches",
                    "1",
                    "audit-cleanup-timeout-seconds",
                    "1"))
            .auditCleanup());
    assertEquals(
        new AuditCleanupConfig(Duration.ofDays(3650), Duration.ofHours(1), 500, 100, 300),
        BridgeConfig.from(
                Map.of(
                    "audit-retention-seconds",
                    "315360000",
                    "audit-cleanup-interval-ms",
                    "3600000",
                    "audit-cleanup-batch-size",
                    "500",
                    "audit-cleanup-max-batches",
                    "100",
                    "audit-cleanup-timeout-seconds",
                    "300"))
            .auditCleanup());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = {-1, 315360001, Long.MAX_VALUE})
  void directRetentionValidationCannotOverflow(Long seconds) {
    Duration retention = seconds == null ? null : Duration.ofSeconds(seconds);
    assertThrows(
        IllegalArgumentException.class,
        () -> new AuditCleanupConfig(retention, Duration.ofMinutes(1), 500, 10, 10));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = {999, 3600001, Long.MAX_VALUE})
  void directIntervalValidationCannotOverflow(Long millis) {
    Duration interval = millis == null ? null : Duration.ofMillis(millis);
    assertThrows(
        IllegalArgumentException.class,
        () -> new AuditCleanupConfig(Duration.ZERO, interval, 500, 10, 10));
  }
}
