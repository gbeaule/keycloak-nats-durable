package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RelayWakeupTest {
  @Test
  void commitBetweenScanAndSleepIsNotLost() {
    var wakeup = new RelayWakeup();
    wakeup.signal();
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertTrue(wakeup.awaitSignal(60000)));
  }

  @Test
  void commitWakesSleepingWorker() throws Exception {
    var wakeup = new RelayWakeup();
    var worker = new AtomicReference<Thread>();
    var executor = Executors.newSingleThreadExecutor();
    try {
      var waiting =
          executor.submit(
              () -> {
                worker.set(Thread.currentThread());
                return wakeup.awaitSignal(60000);
              });
      awaitWaiting(worker);
      wakeup.signal();
      assertTrue(waiting.get(1, TimeUnit.SECONDS));
    } finally {
      wakeup.close();
      executor.shutdownNow();
    }
  }

  @Test
  void closeBeforeWaitWinsOverPendingAndLateSignals() {
    var wakeup = new RelayWakeup();
    wakeup.signal();
    wakeup.close();
    wakeup.signal();
    assertFalse(wakeup.isOpen());
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertFalse(wakeup.awaitSignal(60000)));
  }

  @Test
  void noSignalStillAllowsRecoveryScan() {
    var wakeup = new RelayWakeup();
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertTrue(wakeup.awaitSignal(10)));
  }

  @Test
  void burstOfCommitsConsumesOnlyOneWakeup() throws Exception {
    var wakeup = new RelayWakeup();
    for (int i = 0; i < 10000; i++) {
      wakeup.signal();
    }
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertTrue(wakeup.awaitSignal(60000)));
    var worker = new AtomicReference<Thread>();
    var executor = Executors.newSingleThreadExecutor();
    try {
      var waiting =
          executor.submit(
              () -> {
                worker.set(Thread.currentThread());
                return wakeup.awaitSignal(60000);
              });
      // Accumulating permits would spin through extra empty scans after a burst.
      awaitWaiting(worker);
      wakeup.close();
      assertFalse(waiting.get(1, TimeUnit.SECONDS));
    } finally {
      wakeup.close();
      executor.shutdownNow();
    }
  }

  @Test
  void zeroDelayDrainsBacklogWithoutWaiting() {
    var wakeup = new RelayWakeup();
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertTrue(wakeup.awaitSignal(0)));
  }

  @Test
  void interruptionPropagatesToWorker() {
    var wakeup = new RelayWakeup();
    try {
      Thread.currentThread().interrupt();
      assertThrows(InterruptedException.class, () -> wakeup.awaitSignal(60000));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void cooldownIgnoresPendingAndNewSignalsButClosesPromptly() throws Exception {
    var wakeup = new RelayWakeup();
    wakeup.signal();
    var worker = new AtomicReference<Thread>();
    try (var executor = Executors.newSingleThreadExecutor()) {
      var waiting =
          executor.submit(
              () -> {
                worker.set(Thread.currentThread());
                return wakeup.awaitCooldown(60000);
              });
      try {
        awaitWaiting(worker);
        wakeup.signal();
        awaitWaiting(worker);
        assertFalse(waiting.isDone());
      } finally {
        wakeup.close();
      }
      assertFalse(waiting.get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void cooldownExpiresDespiteQueuedSignals() {
    var wakeup = new RelayWakeup();
    wakeup.signal();
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertTrue(wakeup.awaitCooldown(10)));
  }

  private static void awaitWaiting(AtomicReference<Thread> worker) {
    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> {
          while (worker.get() == null || worker.get().getState() != Thread.State.TIMED_WAITING) {
            if (Thread.currentThread().isInterrupted()) {
              return;
            }
            Thread.yield();
          }
        });
  }
}
