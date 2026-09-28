package io.github.gbeaule.keycloaknats;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;

/** Node counters and cached database observations; scrapes never query the database. */
final class AuditCleanupMetrics implements AutoCloseable {
  private final MeterRegistry registry;
  private final Counter deleted;
  private final Counter failures;
  private final List<Meter> meters;
  private volatile double retainedRows = Double.NaN;
  private volatile double oldestEligibleAge = Double.NaN;
  private volatile double lastSuccess;

  AuditCleanupMetrics(MeterRegistry registry) {
    this.registry = registry;
    deleted = registry.counter("knd.audit.cleanup.deleted");
    failures = registry.counter("knd.audit.cleanup.failures");
    meters =
        List.of(
            deleted,
            failures,
            Gauge.builder("knd.audit.retained.rows", this, m -> m.retainedRows).register(registry),
            Gauge.builder("knd.audit.oldest.eligible.age.seconds", this, m -> m.oldestEligibleAge)
                .register(registry),
            Gauge.builder(
                    "knd.audit.cleanup.last.success.timestamp.seconds", this, m -> m.lastSuccess)
                .register(registry));
  }

  void deleted(int count) {
    deleted.increment(count);
  }

  void succeeded(AuditRepository.Statistics statistics) {
    retainedRows = statistics.retainedRows();
    oldestEligibleAge = statistics.oldestEligibleAgeSeconds();
    lastSuccess = statistics.databaseTime() / 1000.0;
  }

  void failed() {
    failures.increment();
  }

  @Override
  public void close() {
    meters.forEach(registry::remove);
  }
}
