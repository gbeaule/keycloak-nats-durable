package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jboss.logging.Logger;

/**
 * Publishes one locked row per transaction and retains it whenever acknowledgement is uncertain.
 */
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
    NO_WORK,
    STALE,
    STOPPED,
    TRANSACTION_FAILED
  }

  record BatchResult(int processed, int published, int retries, Outcome outcome) {}

  private record Claim(String id, long version) {}

  private final Transactions transactions;
  private final EventPublisher publisher;
  private final BridgeConfig config;
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
    Outcome outcome = Outcome.STOPPED;
    try {
      while (processed < config.batchSize() && !isStopped()) {
        Claim claim = transact(this::prepareNext);
        if (isStopped()) {
          outcome = Outcome.STOPPED;
          break;
        }
        if (claim == null) {
          outcome = Outcome.NO_WORK;
          break;
        }
        processed++;
        outcome = transact(em -> publishPrepared(em, claim));
        if (outcome == Outcome.PUBLISHED) {
          published++;
        } else if (outcome == Outcome.RETRY_SCHEDULED) {
          retries++;
        } else if (outcome == Outcome.STOPPED) {
          break;
        }
      }
    } catch (RuntimeException e) {
      // A failed delete/commit keeps the row. Retrying uses its original message ID.
      logger.errorf(
          "NATS outbox transaction failed; rows retained; %s", NatsDiagnostics.describe(e));
      outcome = Outcome.TRANSACTION_FAILED;
    }
    return new BatchResult(processed, published, retries, outcome);
  }

  private Claim prepareNext(EntityManager em) {
    var next = OutboxRepository.lockNextDue(em, System.currentTimeMillis());
    if (next.isEmpty() || isStopped()) {
      return null;
    }
    OutboxEvent row = next.get();
    // Commit intent before any network operation. Rollback after a send cannot erase ambiguity.
    row.markPublicationIntent();
    em.flush();
    return new Claim(row.id(), row.version());
  }

  private Outcome publishPrepared(EntityManager em, Claim claim) {
    var next =
        OutboxRepository.lockPrepared(em, claim.id(), claim.version(), System.currentTimeMillis());
    // Reacquire the same unchanged head in a fresh transaction, retaining ownership through send.
    if (isStopped()) {
      return Outcome.STOPPED;
    }
    if (next.isEmpty()) {
      return Outcome.STALE;
    }
    OutboxEvent row = next.get();
    try {
      publisher.publish(row);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Outcome.STOPPED;
    } catch (Exception e) {
      if (isStopped()) {
        return Outcome.STOPPED;
      }
      row.failed(
          System.currentTimeMillis() + RetryBackoff.delay(config, row.attempts()),
          e.getClass().getSimpleName());
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
