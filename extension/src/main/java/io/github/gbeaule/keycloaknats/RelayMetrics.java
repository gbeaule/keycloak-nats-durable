package io.github.gbeaule.keycloaknats;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;

/** Per-node activity; resolution counters advance only after the source transaction commits. */
final class RelayMetrics implements AutoCloseable {
  private final MeterRegistry registry;
  private final Counter published;
  private final Counter retries;
  private final Counter failures;
  private final Counter transactionFailures;
  private final Counter expired;
  private final Counter exhausted;
  private final Counter uncertainDiscards;

  RelayMetrics(MeterRegistry registry) {
    this.registry = registry;
    published = registry.counter("knd.publication.confirmed");
    retries = registry.counter("knd.publication.retries");
    failures = registry.counter("knd.publication.failures");
    transactionFailures = registry.counter("knd.publication.transaction.failures");
    expired = registry.counter("knd.publication.discards", "reason", "expired");
    exhausted = registry.counter("knd.publication.discards", "reason", "max_failures");
    uncertainDiscards = registry.counter("knd.publication.discards.unknown");
  }

  void committed(OutboxRelay.Resolution result) {
    switch (result.outcome()) {
      case PUBLISHED -> published.increment();
      case RETRY_SCHEDULED -> retries.increment();
      case DISCARDED_EXPIRED -> expired.increment();
      case DISCARDED_MAX_FAILURES -> exhausted.increment();
      default -> {}
    }
    if (result.uncertainDiscard()) {
      uncertainDiscards.increment();
    }
  }

  void publicationFailed() {
    failures.increment();
  }

  void transactionFailed() {
    transactionFailures.increment();
  }

  @Override
  public void close() {
    List.of(
            published,
            retries,
            failures,
            transactionFailures,
            expired,
            exhausted,
            uncertainDiscards)
        .forEach(registry::remove);
  }
}
