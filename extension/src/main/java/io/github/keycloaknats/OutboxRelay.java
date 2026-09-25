package io.github.keycloaknats;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.util.function.Function;
import org.jboss.logging.Logger;

public final class OutboxRelay implements Runnable {
  private static final Logger LOG = Logger.getLogger(OutboxRelay.class);

  @FunctionalInterface
  public interface Transactions {
    boolean run(Function<EntityManager, Boolean> work);
  }

  private final Transactions transactions;
  private final EventPublisher publisher;
  private final BridgeConfig config;
  private volatile boolean stopped;

  public OutboxRelay(Transactions transactions, EventPublisher publisher, BridgeConfig config) {
    this.transactions = transactions;
    this.publisher = publisher;
    this.config = config;
  }

  @Override
  public void run() {
    try {
      for (int i = 0;
          i < config.batchSize() && !stopped && !Thread.currentThread().isInterrupted();
          i++) {
        if (!transactions.run(this::publishNext)) break;
      }
    } catch (RuntimeException e) {
      // A failed delete/commit keeps the row. Retrying uses its original message ID.
      LOG.errorf(
          "NATS outbox transaction failed; rows retained; category=%s",
          e.getClass().getSimpleName());
    }
  }

  private boolean publishNext(EntityManager em) {
    var rows =
        em.createQuery(
                "select e from NatsOutboxEvent e where e.nextAttemptAt <= :now order by e.nextAttemptAt, e.createdAt, e.id",
                OutboxEvent.class)
            .setParameter("now", System.currentTimeMillis())
            .setMaxResults(1)
            .setLockMode(LockModeType.PESSIMISTIC_WRITE)
            // Hibernate SKIP_LOCKED: other Keycloak instances skip this row until our transaction
            // ends.
            .setHint("jakarta.persistence.lock.timeout", -2)
            .getResultList();
    if (rows.isEmpty()) return false;
    OutboxEvent row = rows.getFirst();
    try {
      publisher.publish(row);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (Exception e) {
      row.failed(
          System.currentTimeMillis() + RetryBackoff.delay(config, row.attempts()),
          e.getClass().getSimpleName());
      if (row.attempts() == 1 || (row.attempts() & (row.attempts() - 1)) == 0)
        LOG.warnf(
            "NATS outbox publish pending; id=%s attempts=%d category=%s",
            row.id(), row.attempts(), row.lastError());
      return false;
    }
    em.remove(row);
    return true;
  }

  public void stop() {
    stopped = true;
  }
}
