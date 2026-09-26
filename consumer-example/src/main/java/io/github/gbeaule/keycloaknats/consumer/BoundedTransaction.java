package io.github.gbeaule.keycloaknats.consumer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

/**
 * Runs independent transactions concurrently and prevents a timed-out handler from later
 * committing.
 */
final class BoundedTransaction {
  @FunctionalInterface
  interface Work<T> {
    T apply(Connection transaction) throws Exception;
  }

  private final DataSource database;
  private final ProcessingLimits limits;
  private final AtomicBoolean unresponsive = new AtomicBoolean();

  BoundedTransaction(DataSource database, ProcessingLimits limits) {
    this.database = database;
    this.limits = limits;
  }

  <T> T execute(Work<T> work) throws Exception {
    if (unresponsive.get()) {
      throw new UnresponsiveHandlerException();
    }
    var attempt = new Attempt();
    var result = new CompletableFuture<T>();
    var stopped = new CountDownLatch(1);
    Thread task =
        Thread.ofVirtual()
            .name("knd-database-attempt")
            .start(
                () -> {
                  try {
                    result.complete(transact(attempt, work));
                  } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                  } finally {
                    stopped.countDown();
                  }
                });
    try {
      return result.get(limits.deadlineMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException | InterruptedException failure) {
      attempt.expire();
      task.interrupt();
      boolean interrupted = failure instanceof InterruptedException;
      try {
        if (!stopped.await(limits.cleanupMs(), TimeUnit.MILLISECONDS)) {
          unresponsive.set(true);
          throw new UnresponsiveHandlerException();
        }
      } catch (InterruptedException cleanupInterrupted) {
        unresponsive.set(true);
        interrupted = true;
        throw new UnresponsiveHandlerException();
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      throw failure;
    } catch (ExecutionException failure) {
      if (failure.getCause() instanceof Exception exception) {
        throw exception;
      }
      if (failure.getCause() instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException("Unexpected processing failure");
    }
  }

  private <T> T transact(Attempt attempt, Work<T> work) throws Exception {
    try (Connection db = database.getConnection()) {
      try {
        attempt.attach(db);
        db.setAutoCommit(false);
        try {
          try (var settings =
              db.prepareStatement(
                  "SELECT set_config('statement_timeout', ?, true),"
                      + " set_config('lock_timeout', ?, true)")) {
            settings.setString(1, Integer.toString(limits.statementMs()));
            settings.setString(2, Integer.toString(limits.lockMs()));
            settings.execute();
          }
          T value = work.apply(db);
          attempt.check();
          db.commit();
          return value;
        } catch (Exception | Error failure) {
          try {
            db.rollback();
          } catch (SQLException rollback) {
            // A failed rollback is an infrastructure failure, even if the handler rejected the
            // event. Never turn this uncertain database outcome into an eligible discard.
            rollback.addSuppressed(failure);
            throw rollback;
          }
          throw failure;
        }
      } finally {
        attempt.detach();
      }
    }
  }

  private final class Attempt {
    private final long started = System.nanoTime();
    private final AtomicBoolean expired = new AtomicBoolean();
    private Connection connection;

    synchronized void attach(Connection db) throws SQLException {
      check();
      connection = db;
    }

    synchronized void detach() {
      connection = null;
    }

    void check() throws SQLTimeoutException {
      if (expired.get()
          || System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(limits.deadlineMs())
          || Thread.currentThread().isInterrupted()) {
        throw new SQLTimeoutException("Processing deadline exceeded");
      }
    }

    void expire() {
      expired.set(true);
      // abort itself may invoke driver code; never run it on the waiting worker's thread.
      Thread.ofVirtual().name("knd-database-abort").start(this::abort);
    }

    private synchronized void abort() {
      // Serialize abort with detach, so a late cancellation cannot abort a reused pool connection.
      if (connection != null) {
        try {
          connection.abort(Runnable::run);
        } catch (SQLException ignored) {
          // No ACK follows an ambiguous outcome. The inbox resolves it on redelivery.
        }
      }
    }
  }
}
