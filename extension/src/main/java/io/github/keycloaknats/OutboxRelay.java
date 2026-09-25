package io.github.keycloaknats;

import jakarta.persistence.EntityManager;
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
    /** Returns the work's result only after its transaction commits successfully. */
    boolean run(Function<EntityManager, Boolean> work);
  }

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

  /** Returns the number of publications whose database removal also committed. */
  int runBatch() {
    int published = 0;
    try {
      for (int i = 0;
          i < config.batchSize() && !stopped && !Thread.currentThread().isInterrupted();
          i++) {
        if (!transactions.run(this::publishNext)) {
          break;
        }
        published++;
      }
    } catch (RuntimeException e) {
      // A failed delete/commit keeps the row. Retrying uses its original message ID.
      logger.errorf(
          "NATS outbox transaction failed; rows retained; %s", NatsDiagnostics.describe(e));
    }
    return published;
  }

  private boolean publishNext(EntityManager em) {
    var next = OutboxRepository.lockNextDue(em, System.currentTimeMillis());
    // Shutdown may begin while opening the transaction or obtaining the row lock.
    if (next.isEmpty() || stopped || Thread.currentThread().isInterrupted()) {
      return false;
    }
    OutboxEvent row = next.get();
    try {
      publisher.publish(row);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      row.failed(
          System.currentTimeMillis() + RetryBackoff.delay(config, row.attempts()),
          e.getClass().getSimpleName());
      if (row.attempts() == 1 || (row.attempts() & (row.attempts() - 1)) == 0) {
        logger.warnf(
            "NATS outbox publish pending; id=%s attempts=%d retryAt=%d reason=%s",
            row.id(), row.attempts(), row.nextAttemptAt(), NatsDiagnostics.describe(e));
      }
      return false;
    }
    em.remove(row);
    return true;
  }

  /** Prevents further claims and publishing after a pending row lookup completes. */
  public void stop() {
    stopped = true;
  }
}
