package io.github.gbeaule.keycloaknats;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManager;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.jboss.logging.Logger;

/** Bounded local maintenance, independent of NATS and publication workers. */
public final class AuditCleanup implements Runnable, AutoCloseable {
  private static final Logger logger = Logger.getLogger(AuditCleanup.class);
  private final OutboxRelay.Transactions transactions;
  private final AuditCleanupConfig config;
  private final AuditCleanupMetrics metrics;
  private ScheduledExecutorService executor;
  private volatile boolean stopped;

  /** The transaction runner must enforce the configured timeout and return only after commit. */
  public AuditCleanup(
      OutboxRelay.Transactions transactions, AuditCleanupConfig config, MeterRegistry registry) {
    this.transactions = transactions;
    this.config = config;
    this.metrics = new AuditCleanupMetrics(registry);
  }

  /** Starts one nonoverlapping schedule for this node after schema installation. */
  public synchronized void start() {
    if (executor != null || stopped) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            work -> {
              Thread thread = new Thread(work, "keycloak-nats-audit-cleanup");
              thread.setDaemon(true);
              return thread;
            });
    executor.scheduleWithFixedDelay(this, 0, config.interval().toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void run() {
    if (isStopped()) {
      return;
    }
    try {
      for (int batch = 0; batch < config.maxBatches(); batch++) {
        int deleted =
            transact(
                em -> {
                  long cutoff = CaptureRepository.databaseTime(em) - config.retention().toMillis();
                  var rows = AuditRepository.lockExpired(em, cutoff, config.batchSize());
                  checkRunning();
                  rows.forEach(em::remove);
                  return rows.size();
                });
        metrics.deleted(deleted);
        if (deleted < config.batchSize()) {
          break;
        }
      }
      // A separate bounded read keeps aggregate scans out of deletion transactions.
      metrics.succeeded(transact(em -> AuditRepository.statistics(em, config.retention())));
    } catch (RuntimeException failure) {
      metrics.failed();
      logger.warnf(
          "NATS audit cleanup failed; retrying next sweep; %s", NatsDiagnostics.describe(failure));
    }
  }

  private <T> T transact(Function<EntityManager, T> work) {
    checkRunning();
    var result = new AtomicReference<T>();
    transactions.run(
        em -> {
          AuditRepository.limitStatements(em, config.timeoutSeconds());
          checkRunning();
          result.set(work.apply(em));
          em.flush();
          checkRunning();
        });
    return result.get();
  }

  private boolean isStopped() {
    return stopped || Thread.currentThread().isInterrupted();
  }

  private void checkRunning() {
    if (isStopped()) {
      throw new CancellationException("Audit cleanup stopped");
    }
  }

  @Override
  public void close() {
    ScheduledExecutorService stopping;
    synchronized (this) {
      if (stopped) {
        return;
      }
      stopped = true;
      stopping = executor;
    }
    if (stopping != null) {
      stopping.shutdownNow();
      try {
        if (!stopping.awaitTermination(5, TimeUnit.SECONDS)) {
          logger.warn("NATS audit cleanup shutdown is still completing; audits remain recoverable");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    metrics.close();
  }
}
