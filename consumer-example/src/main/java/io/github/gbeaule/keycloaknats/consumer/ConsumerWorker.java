package io.github.gbeaule.keycloaknats.consumer;

import io.nats.client.Message;
import java.sql.SQLTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Processes one delivery at a time; multiple replicas share the inbox and broker pending window.
 */
public final class ConsumerWorker implements AutoCloseable {
  private final InboxProcessor processor;
  private final QuarantineStore quarantine;
  private final ConsumerSettings settings;
  private final ConsumerMonitor monitor;
  private final ScheduledExecutorService progress =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("knd-progress-ack").factory());

  /** Shares explicit policies with the monitor and the database processing implementation. */
  public ConsumerWorker(
      InboxProcessor processor,
      QuarantineStore quarantine,
      ConsumerSettings settings,
      ConsumerMonitor monitor) {
    this.processor = processor;
    this.quarantine = quarantine;
    this.settings = settings;
    this.monitor = monitor;
  }

  /**
   * Never acknowledges a failed transaction, timeout, ambiguous commit or failed quarantine write.
   */
  public void handle(Message message, InboxProcessor.Handler handler) throws InterruptedException {
    var attempt = monitor.begin();
    var heartbeat =
        progress.scheduleAtFixedRate(
            () -> {
              try {
                message.inProgress();
              } catch (RuntimeException failure) {
                monitor.increment(ConsumerMonitor.Counter.PROGRESS_FAILURES);
              }
            },
            settings.progressMs(),
            settings.progressMs(),
            TimeUnit.MILLISECONDS);
    try {
      boolean acknowledge;
      try {
        var metadata = message.metaData();
        if (settings
            .failures()
            .expired(message.getSubject(), metadata.timestamp().toInstant(), Instant.now())) {
          quarantine.record(message, FailurePolicy.Action.DROP, "AGE_LIMIT");
          monitor.increment(ConsumerMonitor.Counter.DROPPED);
        } else {
          try {
            boolean inserted = processor.process(message.getData(), handler);
            monitor.increment(
                inserted ? ConsumerMonitor.Counter.COMMITTED : ConsumerMonitor.Counter.DUPLICATES);
          } catch (RejectedEventException rejected) {
            FailurePolicy.Action action =
                settings.failures().rejected(message.getSubject(), metadata.deliveredCount());
            if (action == FailurePolicy.Action.RETRY) {
              throw rejected;
            }
            quarantine.record(message, action, rejected.reason().name());
            monitor.increment(
                action == FailurePolicy.Action.DROP
                    ? ConsumerMonitor.Counter.DROPPED
                    : ConsumerMonitor.Counter.QUARANTINED);
          }
        }
        acknowledge = true;
      } catch (UnresponsiveHandlerException failure) {
        monitor.failed();
        throw failure;
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw interrupted;
      } catch (Exception failure) {
        monitor.increment(ConsumerMonitor.Counter.RETRIES);
        if (failure instanceof TimeoutException
            || failure instanceof SQLTimeoutException
            || (failure instanceof java.sql.SQLException sql
                && ("57014".equals(sql.getSQLState()) || "55P03".equals(sql.getSQLState())))) {
          monitor.increment(ConsumerMonitor.Counter.TIMEOUTS);
        }
        // Keep the original delivery pending. Logs intentionally contain no parser or driver text.
        acknowledge = false;
      }
      if (acknowledge) {
        try {
          message.ackSync(Duration.ofMillis(settings.ackTimeoutMs()));
        } catch (TimeoutException | RuntimeException failure) {
          // This is separate from application failure: a committed effect is never a poison event.
          monitor.increment(ConsumerMonitor.Counter.ACK_FAILURES);
        }
      }
    } finally {
      heartbeat.cancel(false);
      monitor.end(attempt);
    }
  }

  @Override
  public void close() {
    progress.shutdownNow();
  }
}
