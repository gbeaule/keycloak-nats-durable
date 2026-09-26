package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class BoundedTransactionTest {
  private final AtomicInteger commits = new AtomicInteger();
  private final CountDownLatch closed = new CountDownLatch(1);
  private final CountDownLatch aborted = new CountDownLatch(1);
  private boolean failRollback;

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
            (proxy, method, args) -> "execute".equals(method.getName()) ? false : null);
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "prepareStatement":
                  return statement;
                case "commit":
                  commits.incrementAndGet();
                  break;
                case "rollback":
                  if (failRollback) {
                    throw new java.sql.SQLException("Test rollback failed", "08006");
                  }
                  break;
                case "close":
                  closed.countDown();
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
