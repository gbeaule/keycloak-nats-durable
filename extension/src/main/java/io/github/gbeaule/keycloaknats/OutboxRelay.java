package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;

/** Resolves locked rows through confirmed publication or policy-authorized local discard. */
public final class OutboxRelay implements Runnable {
  private static final Logger logger = Logger.getLogger(OutboxRelay.class);

  enum Outcome {
    PUBLISHED,
    RETRY_SCHEDULED,
    DISCARDED_EXPIRED,
    DISCARDED_MAX_FAILURES,
    NO_WORK,
    STALE,
    STOPPED,
    TRANSACTION_FAILED
  }

  /** All resolution counts are reported only after a successful database commit. */
  record BatchResult(
      int processed, int published, int retries, int expired, int exhausted, Outcome outcome) {
    int discarded() {
      return expired + exhausted;
    }
  }

  private sealed interface Preparation permits Claim, Resolution {}

  private record Claim(String id, long version) implements Preparation {}

  record Resolution(Outcome outcome, boolean uncertainDiscard) implements Preparation {
    Resolution(Outcome outcome) {
      this(outcome, false);
    }

    static Resolution discarded(OutboxEvent row, DiscardReason reason) {
      return new Resolution(
          reason == DiscardReason.EXPIRED
              ? Outcome.DISCARDED_EXPIRED
              : Outcome.DISCARDED_MAX_FAILURES,
          row.publicationMayHaveOccurred());
    }
  }

  private final Transactions transactions;
  private final EventPublisher publisher;
  private final BridgeConfig config;
  private final RelayMetrics metrics;
  private boolean preferExpiry = true;
  private volatile boolean stopped;

  /** Connects transaction ownership, confirmed publication and bounded batch settings. */
  public OutboxRelay(Transactions transactions, EventPublisher publisher, BridgeConfig config) {
    this(transactions, publisher, config, null);
  }

  OutboxRelay(
      Transactions transactions,
      EventPublisher publisher,
      BridgeConfig config,
      RelayMetrics metrics) {
    this.transactions = transactions;
    this.publisher = publisher;
    this.config = config;
    this.metrics = metrics;
  }

  @Override
  public void run() {
    runBatch();
  }

  /** Counts committed results; stale claims also consume the bounded work allowance. */
  BatchResult runBatch() {
    int processed = 0;
    int published = 0;
    int retries = 0;
    int expired = 0;
    int exhausted = 0;
    Outcome outcome = Outcome.STOPPED;
    var unresolved = new AtomicReference<Claim>();
    try {
      while (processed < config.batchSize() && !isStopped()) {
        unresolved.set(null);
        Preparation prepared = transactions.commit(em -> prepareNext(em, unresolved));
        unresolved.set(null);
        if (prepared instanceof Claim && isStopped()) {
          outcome = Outcome.STOPPED;
          break;
        }
        if (prepared instanceof Resolution result
            && (result.outcome() == Outcome.NO_WORK || result.outcome() == Outcome.STOPPED)) {
          outcome = result.outcome();
          break;
        }
        processed++;
        Resolution resolution =
            switch (prepared) {
              case Claim claim -> transactions.commit(em -> publishPrepared(em, claim, unresolved));
              case Resolution result -> result;
            };
        if (metrics != null) {
          metrics.committed(resolution);
        }
        outcome = resolution.outcome();
        if (outcome == Outcome.PUBLISHED) {
          published++;
        } else if (outcome == Outcome.RETRY_SCHEDULED) {
          retries++;
        } else if (outcome == Outcome.DISCARDED_EXPIRED) {
          expired++;
        } else if (outcome == Outcome.DISCARDED_MAX_FAILURES) {
          exhausted++;
        } else if (outcome == Outcome.STOPPED) {
          break;
        }
      }
    } catch (RuntimeException e) {
      // An uncertain commit is not counted as resolution. Recheck persisted state on the next run.
      logger.errorf(
          "NATS outbox transaction failed; resolution unconfirmed; %s",
          NatsDiagnostics.describe(e));
      outcome = Outcome.TRANSACTION_FAILED;
      if (metrics != null) {
        metrics.transactionFailed();
      }
      if (unresolved.get() != null) {
        deferFailedResolution(unresolved.get());
      }
    }
    return new BatchResult(processed, published, retries, expired, exhausted, outcome);
  }

  private Preparation prepareNext(EntityManager em, AtomicReference<Claim> unresolved) {
    long now = CaptureRepository.readDatabaseTime(em);
    // Share the batch allowance fairly, including when batch-size is one.
    boolean expiryFirst = preferExpiry;
    preferExpiry = !preferExpiry;
    var next =
        expiryFirst
            ? OutboxRepository.lockNextExpired(em, now)
            : OutboxRepository.lockNextDue(em, now);
    if (next.isEmpty() && !isStopped()) {
      next =
          expiryFirst
              ? OutboxRepository.lockNextDue(em, now)
              : OutboxRepository.lockNextExpired(em, now);
    }
    if (isStopped()) {
      return new Resolution(Outcome.STOPPED);
    }
    if (next.isEmpty()) {
      return new Resolution(Outcome.NO_WORK);
    }
    OutboxEvent row = next.get();
    unresolved.set(new Claim(row.id(), row.version()));
    var reason = OutboxRepository.discardIfEligible(em, row, now);
    if (reason != null) {
      return Resolution.discarded(row, reason);
    }
    // Commit intent before any network operation. Rollback after a send cannot erase ambiguity.
    row.markPublicationIntent();
    em.flush();
    return new Claim(row.id(), row.version());
  }

  private Resolution publishPrepared(
      EntityManager em, Claim claim, AtomicReference<Claim> unresolved) {
    var next =
        OutboxRepository.lockPrepared(
            em, claim.id(), claim.version(), CaptureRepository.readDatabaseTime(em));
    // Reacquire the same unchanged head in a fresh transaction, retaining ownership through send.
    if (isStopped()) {
      return new Resolution(Outcome.STOPPED);
    }
    if (next.isEmpty()) {
      return new Resolution(Outcome.STALE);
    }
    OutboxEvent row = next.get();
    unresolved.set(new Claim(row.id(), row.version()));
    var reason =
        OutboxRepository.discardIfEligible(em, row, CaptureRepository.readDatabaseTime(em));
    if (reason != null) {
      return Resolution.discarded(row, reason);
    }
    try {
      publisher.publish(row);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Resolution(Outcome.STOPPED);
    } catch (Exception e) {
      if (isStopped()) {
        return new Resolution(Outcome.STOPPED);
      }
      if (metrics != null) {
        metrics.publicationFailed();
      }
      long now = CaptureRepository.readDatabaseTime(em);
      reason =
          OutboxRepository.recordFailureAndScheduleRetry(
              em,
              row,
              now,
              now + RetryBackoff.sampleDelay(config, row.attempts()),
              e.getClass().getSimpleName());
      if (reason != null) {
        return Resolution.discarded(row, reason);
      }
      if (row.attempts() == 1 || (row.attempts() & (row.attempts() - 1)) == 0) {
        logger.warnf(
            "NATS outbox publish pending; id=%s attempts=%d retryAt=%d reason=%s",
            row.id(), row.attempts(), row.nextAttemptAt(), NatsDiagnostics.describe(e));
      }
      return new Resolution(Outcome.RETRY_SCHEDULED);
    }
    OutboxRepository.remove(em, row);
    return new Resolution(Outcome.PUBLISHED);
  }

  private void deferFailedResolution(Claim claim) {
    try {
      // The failed transaction has ended. Never update its detached row or a newer owner's state.
      transactions.run(
          em ->
              OutboxRepository.lockUnchanged(em, claim.id(), claim.version())
                  .ifPresent(
                      row ->
                          OutboxRepository.deferResolution(
                              em,
                              row,
                              CaptureRepository.readDatabaseTime(em)
                                  + RetryBackoff.sampleDelay(config, 0))));
    } catch (RuntimeException e) {
      // A database-wide failure may also prevent backoff; leave the original unresolved.
      logger.errorf("NATS outbox resolution backoff failed; %s", NatsDiagnostics.describe(e));
    }
  }

  private boolean isStopped() {
    return stopped || Thread.currentThread().isInterrupted();
  }

  /** Prevents further claims and publishing after a pending row lookup completes. */
  public void stop() {
    stopped = true;
  }
}
