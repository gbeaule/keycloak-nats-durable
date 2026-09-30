package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class BoundedTransactionTest {
  private final AtomicInteger commits = new AtomicInteger();
  private final AtomicInteger rollbacks = new AtomicInteger();
  private final CountDownLatch closed = new CountDownLatch(1);
  private final CountDownLatch aborted = new CountDownLatch(1);
  private boolean failRollback;
  private SQLException commitFailure;
  private boolean autoCommit = true;
  private boolean settingsApplied;
  private final Map<Integer, String> statementParameters = new HashMap<>();
  private Runnable onClose = () -> {};

  @Test
  void callerInterruptionCannotCommitAfterTheHandlerClearsItsInterrupt() throws Exception {
    var entered = new CountDownLatch(1);
    var cancelled = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var returned = new CountDownLatch(1);
    var failure = new AtomicReference<Throwable>();
    var interruptPreserved = new AtomicBoolean();
    var transactions = new BoundedTransaction(database(this::connection), limits(10000, 2000));
    Thread caller =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    transactions.execute(
                        db -> {
                          entered.countDown();
                          try {
                            release.await(5, TimeUnit.SECONDS);
                          } catch (InterruptedException ignored) {
                            cancelled.countDown();
                            ignoringInterrupts(release);
                          }
                          return true;
                        });
                  } catch (Throwable caught) {
                    failure.set(caught);
                    interruptPreserved.set(Thread.currentThread().isInterrupted());
                  } finally {
                    returned.countDown();
                  }
                });
    try {
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      caller.interrupt();
      assertTrue(cancelled.await(2, TimeUnit.SECONDS));
      release.countDown();
      assertTrue(returned.await(3, TimeUnit.SECONDS));
      assertInstanceOf(InterruptedException.class, failure.get());
      assertTrue(interruptPreserved.get());
      assertEquals(0, commits.get());
      assertEquals(1, rollbacks.get());
      assertEquals(0, closed.getCount());
    } finally {
      release.countDown();
      caller.join(3000);
    }
  }

  @Test
  void lateCancellationCannotAbortReleasedConnections() throws Exception {
    var release = new CountDownLatch(1);
    var finished = new CountDownLatch(1);
    onClose =
        () -> {
          // The pool has reclaimed the connection, but close's remaining bookkeeping stalls.
          ignoringInterrupts(release);
          finished.countDown();
        };
    var transactions = new BoundedTransaction(database(this::connection), limits(200, 100));
    try {
      assertThrows(UnresponsiveHandlerException.class, () -> transactions.execute(db -> true));
      assertEquals(0, closed.getCount());
      assertFalse(
          aborted.await(1, TimeUnit.SECONDS), "A released connection may have another owner");
    } finally {
      release.countDown();
    }
    assertTrue(finished.await(1, TimeUnit.SECONDS));
    assertEquals(1, commits.get());
  }

  @Test
  void successfulWorkReturnsItsValueOnlyAfterCommitAndClose() throws Exception {
    var transactions = new BoundedTransaction(database(this::connection), limits(5000, 1000));
    var result = new Object();
    assertSame(
        result,
        transactions.execute(
            db -> {
              assertFalse(autoCommit);
              assertTrue(settingsApplied);
              assertEquals(Map.of(1, "1000", 2, "100"), statementParameters);
              assertEquals(0, commits.get());
              assertEquals(1, closed.getCount());
              return result;
            }));
    assertEquals(1, commits.get());
    assertEquals(0, rollbacks.get());
    assertEquals(0, closed.getCount());
  }

  @Test
  void failedCommitCannotReturnTheHandlersSuccessfulResult() {
    commitFailure = new SQLException("Commit acknowledgement lost", "08006");
    var transactions = new BoundedTransaction(database(this::connection), limits(5000, 1000));
    assertSame(
        commitFailure, assertThrows(SQLException.class, () -> transactions.execute(db -> true)));
    assertEquals(1, commits.get());
    assertEquals(1, rollbacks.get());
    assertEquals(0, closed.getCount());
  }

  @Test
  void handlerRejectionIsReturnedOnlyAfterRollbackAndClose() {
    var rejection = new RejectedEventException(RejectedEventException.Reason.HANDLER_REJECTED);
    var transactions = new BoundedTransaction(database(this::connection), limits(5000, 1000));
    assertSame(
        rejection,
        assertThrows(
            RejectedEventException.class,
            () ->
                transactions.execute(
                    db -> {
                      throw rejection;
                    })));
    assertEquals(0, commits.get());
    assertEquals(1, rollbacks.get());
    assertEquals(0, closed.getCount());
  }

  @Test
  void failedRollbackDoesNotExposeAnEligibleRejection() {
    failRollback = true;
    var transactions = new BoundedTransaction(database(this::connection), limits(1000, 1000));
    var failure =
        assertThrows(
            java.sql.SQLException.class,
            () ->
                transactions.execute(
                    db -> {
                      throw new RejectedEventException(
                          RejectedEventException.Reason.HANDLER_REJECTED);
                    }));
    assertEquals(1, failure.getSuppressed().length);
    assertTrue(failure.getSuppressed()[0] instanceof RejectedEventException);
    assertEquals(0, commits.get());
    assertEquals(1, rollbacks.get());
    assertEquals(0, closed.getCount());
  }

  @Test
  void cooperativeTimeoutRollsBackAndDoesNotCommit() throws Exception {
    var transactions = new BoundedTransaction(database(this::connection), limits(200, 1000));
    long started = System.nanoTime();
    assertThrows(
        TimeoutException.class,
        () ->
            transactions.execute(
                db -> {
                  Thread.sleep(30000);
                  return true;
                }));
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 3000);
    assertTrue(closed.await(1, TimeUnit.SECONDS));
    assertEquals(0, commits.get());
    assertEquals(1, rollbacks.get());
  }

  @Test
  void uncooperativeHandlerCannotCommitLaterOrAccumulateFurtherAttempts() throws Exception {
    var release = new CountDownLatch(1);
    var transactions = new BoundedTransaction(database(this::connection), limits(200, 100));
    try {
      assertThrows(
          UnresponsiveHandlerException.class,
          () ->
              transactions.execute(
                  db -> {
                    ignoringInterrupts(release);
                    return true;
                  }));
      assertTrue(aborted.await(1, TimeUnit.SECONDS));
      assertThrows(UnresponsiveHandlerException.class, () -> transactions.execute(db -> true));
    } finally {
      release.countDown();
    }
    assertTrue(closed.await(1, TimeUnit.SECONDS));
    assertEquals(0, commits.get());
  }

  @Test
  void connectionArrivingAfterTheDeadlineNeverRunsTheHandler() throws Exception {
    var release = new CountDownLatch(1);
    var invocations = new AtomicInteger();
    var transactions =
        new BoundedTransaction(
            database(
                () -> {
                  ignoringInterrupts(release);
                  return connection();
                }),
            limits(100, 100));
    try {
      assertThrows(
          UnresponsiveHandlerException.class,
          () -> transactions.execute(db -> invocations.incrementAndGet()));
    } finally {
      release.countDown();
    }
    assertTrue(closed.await(1, TimeUnit.SECONDS));
    assertEquals(0, invocations.get());
    assertEquals(0, commits.get());
  }

  private static ProcessingLimits limits(int deadline, int cleanup) {
    return new ProcessingLimits(1, 1, 1000, 100, deadline, cleanup);
  }

  private static void ignoringInterrupts(CountDownLatch release) {
    boolean done = false;
    while (!done) {
      try {
        done = release.await(5, TimeUnit.SECONDS);
        if (!done) {
          throw new AssertionError("Test did not release handler");
        }
      } catch (InterruptedException ignored) {
        // Deliberately model a broken handler that ignores cancellation.
      }
    }
  }

  private static DataSource database(Supplier<Connection> connection) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if ("getConnection".equals(method.getName())) {
                return connection.get();
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private Connection connection() {
    var statement =
        Proxy.newProxyInstance(
            PreparedStatement.class.getClassLoader(),
            new Class<?>[] {PreparedStatement.class},
            (proxy, method, args) -> {
              if ("setString".equals(method.getName())) {
                statementParameters.put((Integer) args[0], (String) args[1]);
              }
              if ("execute".equals(method.getName())) {
                settingsApplied = true;
                return false;
              }
              return null;
            });
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "setAutoCommit":
                  autoCommit = (boolean) args[0];
                  break;
                case "prepareStatement":
                  return statement;
                case "commit":
                  commits.incrementAndGet();
                  if (commitFailure != null) {
                    throw commitFailure;
                  }
                  break;
                case "rollback":
                  rollbacks.incrementAndGet();
                  if (failRollback) {
                    throw new java.sql.SQLException("Test rollback failed", "08006");
                  }
                  break;
                case "close":
                  closed.countDown();
                  onClose.run();
                  break;
                case "abort":
                  aborted.countDown();
                  break;
                default:
                  break;
              }
              return null;
            });
  }
}
