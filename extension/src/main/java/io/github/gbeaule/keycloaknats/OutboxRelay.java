package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jboss.logging.Logger;

/** Resolves locked rows through confirmed publication or policy-authorized local discard. */
public final class OutboxRelay implements Runnable {
  private static final Logger logger = Logger.getLogger(OutboxRelay.class);

  /** Opens a fresh Keycloak session and commits or rolls back the supplied database work. */
  @FunctionalInterface
  public interface Transactions {
    /** Returns only after commit; each call owns a fresh persistence context. */
    void run(Consumer<EntityManager> work);
  }

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

  private record Claim(String id, long version) {}

  private record Preparation(Claim claim, Outcome outcome) {}

  private final Transactions transactions;
  private final EventPublisher publisher;
  private final BridgeConfig config;
  private boolean preferExpiry = true;
  private volatile boolean stopped;

  /** Connects transaction ownership, confirmed publication and bounded batch settings. */
  public OutboxRelay(Transactions transactions, EventPublisher publisher, BridgeConfig config) {
    this.transactions = transactions;
    this.publisher = publisher;
    this.config = config;
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
    try {
      while (processed < config.batchSize() && !isStopped()) {
        Preparation prepared = transact(this::prepareNext);
        if (prepared.claim() != null && isStopped()) {
          outcome = Outcome.STOPPED;
          break;
        }
        if (prepared.outcome() == Outcome.NO_WORK || prepared.outcome() == Outcome.STOPPED) {
          outcome = prepared.outcome();
          break;
        }
        processed++;
        outcome =
            prepared.claim() == null
                ? prepared.outcome()
                : transact(em -> publishPrepared(em, prepared.claim()));
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
    }
    return new BatchResult(processed, published, retries, expired, exhausted, outcome);
  }

  private Preparation prepareNext(EntityManager em) {
    long now = CaptureRepository.databaseTime(em);
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
      return new Preparation(null, Outcome.STOPPED);
    }
    if (next.isEmpty()) {
      return new Preparation(null, Outcome.NO_WORK);
    }
    OutboxEvent row = next.get();
    Outcome discarded = discardIfEligible(em, row, now);
    if (discarded != null) {
      return new Preparation(null, discarded);
    }
    // Commit intent before any network operation. Rollback after a send cannot erase ambiguity.
    row.markPublicationIntent();
    em.flush();
    return new Preparation(new Claim(row.id(), row.version()), null);
  }

  private Outcome publishPrepared(EntityManager em, Claim claim) {
    var next =
        OutboxRepository.lockPrepared(
            em, claim.id(), claim.version(), CaptureRepository.databaseTime(em));
    // Reacquire the same unchanged head in a fresh transaction, retaining ownership through send.
    if (isStopped()) {
      return Outcome.STOPPED;
    }
    if (next.isEmpty()) {
      return Outcome.STALE;
    }
    OutboxEvent row = next.get();
    Outcome discarded = discardIfEligible(em, row, CaptureRepository.databaseTime(em));
    if (discarded != null) {
      return discarded;
    }
    try {
      publisher.publish(row);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Outcome.STOPPED;
    } catch (Exception e) {
      if (isStopped()) {
        return Outcome.STOPPED;
      }
      long now = CaptureRepository.databaseTime(em);
      row.failed(now + RetryBackoff.delay(config, row.attempts()), e.getClass().getSimpleName());
      discarded = discardIfEligible(em, row, now);
      if (discarded != null) {
        return discarded;
      }
      if (row.attempts() == 1 || (row.attempts() & (row.attempts() - 1)) == 0) {
        logger.warnf(
            "NATS outbox publish pending; id=%s attempts=%d retryAt=%d reason=%s",
            row.id(), row.attempts(), row.nextAttemptAt(), NatsDiagnostics.describe(e));
      }
      return Outcome.RETRY_SCHEDULED;
    }
    em.remove(row);
    return Outcome.PUBLISHED;
  }

  private Outcome discardIfEligible(EntityManager em, OutboxEvent row, long now) {
    var reason =
        row.publicationPolicy().policy().discardReason(row.createdAt(), now, row.attempts());
    if (reason == null) {
      return null;
    }
    OutboxRepository.discard(em, row, reason, now);
    return reason == DiscardReason.EXPIRED
        ? Outcome.DISCARDED_EXPIRED
        : Outcome.DISCARDED_MAX_FAILURES;
  }

  private <T> T transact(Function<EntityManager, T> work) {
    var result = new AtomicReference<T>();
    transactions.run(em -> result.set(work.apply(em)));
    return result.get();
  }

  private boolean isStopped() {
    return stopped || Thread.currentThread().isInterrupted();
  }

  /** Prevents further claims and publishing after a pending row lookup completes. */
  public void stop() {
    stopped = true;
  }
}
