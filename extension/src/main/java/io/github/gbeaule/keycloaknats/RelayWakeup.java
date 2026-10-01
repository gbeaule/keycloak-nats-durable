package io.github.gbeaule.keycloaknats;

import java.util.concurrent.TimeUnit;

/** A single-worker, coalescing wakeup. The database remains the source of truth. */
final class RelayWakeup {
  private boolean pending;
  private boolean closed;

  synchronized boolean isOpen() {
    return !closed;
  }

  synchronized void signal() {
    if (!closed) {
      pending = true;
      notifyAll();
    }
  }

  synchronized boolean awaitSignal(long timeoutMillis) throws InterruptedException {
    return await(timeoutMillis, true);
  }

  synchronized boolean awaitCooldown(long timeoutMillis) throws InterruptedException {
    return await(timeoutMillis, false);
  }

  private boolean await(long timeoutMillis, boolean acceptSignals) throws InterruptedException {
    long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    long started = System.nanoTime();
    long remaining;
    // Subtract readings to handle nanoTime's signed wrap; no lifetime counter is needed.
    while (!closed
        && !(acceptSignals && pending)
        && (remaining = timeoutNanos - (System.nanoTime() - started)) > 0) {
      TimeUnit.NANOSECONDS.timedWait(this, remaining);
    }
    // Only the waiting worker consumes a signal, so a commit during its scan cannot be lost.
    pending = false;
    return !closed;
  }

  synchronized void close() {
    closed = true;
    pending = false;
    notifyAll();
  }
}
