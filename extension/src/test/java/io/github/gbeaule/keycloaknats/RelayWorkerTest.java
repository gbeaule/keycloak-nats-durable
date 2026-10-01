package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RelayWorkerTest {
  @Test
  void interruptedWorkerNeverStartsScanningOrWaiting() {
    var relay = mock(OutboxRelay.class);
    var wakeup = mock(RelayWakeup.class);
    when(wakeup.isOpen()).thenReturn(true);
    try {
      Thread.currentThread().interrupt();
      new RelayWorker(relay, wakeup, BridgeConfig.from(Map.of())).run();
      assertTrue(Thread.currentThread().isInterrupted());
      verifyNoInteractions(relay);
      verify(wakeup).isOpen();
      verifyNoMoreInteractions(wakeup);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void interruptedWaitStopsScanningAndRestoresTheInterrupt() throws Exception {
    var relay = mock(OutboxRelay.class);
    var wakeup = mock(RelayWakeup.class);
    when(wakeup.isOpen()).thenReturn(true);
    when(wakeup.awaitSignal(anyLong())).thenThrow(new InterruptedException());
    when(relay.runBatch()).thenReturn(batch(0));
    try {
      new RelayWorker(relay, wakeup, BridgeConfig.from(Map.of())).run();
      assertTrue(Thread.currentThread().isInterrupted());
      verify(relay).runBatch();
      verifyNoMoreInteractions(relay);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void idleBackoffIsCappedAndResetsAfterPartialAndFullBatches() throws Exception {
    var relay = mock(OutboxRelay.class);
    var wakeup = mock(RelayWakeup.class);
    when(wakeup.isOpen()).thenReturn(true);
    when(relay.runBatch())
        .thenReturn(batch(0), batch(0), batch(0), batch(0), batch(1), batch(2), batch(0));
    when(wakeup.awaitSignal(anyLong())).thenReturn(true, true, true, true, true, true, false);
    var config =
        BridgeConfig.from(Map.of("batch-size", "2", "poll-ms", "100", "idle-poll-max-ms", "500"));
    new RelayWorker(relay, wakeup, config).run();
    var order = inOrder(relay, wakeup);
    for (long delay : new long[] {200, 400, 500, 500, 100, 0, 100}) {
      order.verify(wakeup).isOpen();
      order.verify(relay).runBatch();
      order.verify(wakeup).awaitSignal(delay);
    }
    order.verifyNoMoreInteractions();
  }

  @Test
  void closedWorkerNeverStartsScanning() {
    var relay = mock(OutboxRelay.class);
    var wakeup = new RelayWakeup();
    wakeup.close();
    new RelayWorker(relay, wakeup, BridgeConfig.from(Map.of())).run();
    verifyNoInteractions(relay);
  }

  @Test
  void commitAfterEmptyScanTriggersAnotherScanWithoutPollingDelay() throws Exception {
    var relay = mock(OutboxRelay.class);
    var wakeup = new RelayWakeup();
    var emptyScan = new CountDownLatch(1);
    var finishScan = new CountDownLatch(1);
    var recovered = new CountDownLatch(1);
    doAnswer(
            call -> {
              emptyScan.countDown();
              assertTrue(finishScan.await(2, TimeUnit.SECONDS));
              return batch(0);
            })
        .doAnswer(
            call -> {
              recovered.countDown();
              wakeup.close();
              return batch(1);
            })
        .when(relay)
        .runBatch();
    var config = BridgeConfig.from(Map.of("poll-ms", "60000", "idle-poll-max-ms", "60000"));
    var executor = Executors.newSingleThreadExecutor();
    try {
      final var running = executor.submit(new RelayWorker(relay, wakeup, config));
      assertTrue(emptyScan.await(2, TimeUnit.SECONDS));
      wakeup.signal();
      finishScan.countDown();
      assertTrue(recovered.await(2, TimeUnit.SECONDS));
      running.get(2, TimeUnit.SECONDS);
      verify(relay, times(2)).runBatch();
    } finally {
      wakeup.close();
      finishScan.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void fullBatchContinuesWithoutPollingDelay() throws Exception {
    var relay = mock(OutboxRelay.class);
    var wakeup = new RelayWakeup();
    var config = BridgeConfig.from(Map.of("poll-ms", "60000", "idle-poll-max-ms", "60000"));
    doAnswer(call -> batch(config.batchSize()))
        .doAnswer(
            call -> {
              wakeup.close();
              return batch(0);
            })
        .when(relay)
        .runBatch();
    var executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(new RelayWorker(relay, wakeup, config)).get(2, TimeUnit.SECONDS);
      verify(relay, times(2)).runBatch();
    } finally {
      wakeup.close();
      executor.shutdownNow();
    }
  }

  @Test
  void stoppedBatchExitsWithoutWaiting() {
    var relay = mock(OutboxRelay.class);
    var wakeup = mock(RelayWakeup.class);
    when(wakeup.isOpen()).thenReturn(true);
    when(relay.runBatch())
        .thenReturn(new OutboxRelay.BatchResult(0, 0, 0, 0, 0, OutboxRelay.Outcome.STOPPED));
    new RelayWorker(relay, wakeup, BridgeConfig.from(Map.of())).run();
    verify(wakeup).isOpen();
    verifyNoMoreInteractions(wakeup);
  }

  private static OutboxRelay.BatchResult batch(int processed) {
    // Failed publications count as useful work for polling just like successful ones.
    return new OutboxRelay.BatchResult(
        processed,
        0,
        processed,
        0,
        0,
        processed == 0 ? OutboxRelay.Outcome.NO_WORK : OutboxRelay.Outcome.RETRY_SCHEDULED);
  }

  @Test
  void failedDatabaseCommitDoesNotStartAnImmediateRescan() throws Exception {
    var relay = mock(OutboxRelay.class);
    var wakeup = mock(RelayWakeup.class);
    final var config = BridgeConfig.from(Map.of("batch-size", "1", "poll-ms", "100"));
    when(wakeup.isOpen()).thenReturn(true);
    when(relay.runBatch())
        .thenReturn(
            new OutboxRelay.BatchResult(1, 0, 0, 0, 0, OutboxRelay.Outcome.TRANSACTION_FAILED));
    when(wakeup.awaitCooldown(org.mockito.ArgumentMatchers.anyLong())).thenReturn(false);
    new RelayWorker(relay, wakeup, config).run();
    var delay = org.mockito.ArgumentCaptor.forClass(Long.class);
    verify(wakeup).awaitCooldown(delay.capture());
    assertTrue(delay.getValue() >= 500 && delay.getValue() <= 1000);
    verify(relay).runBatch();
  }

  @Test
  void repeatedFailuresIncreaseCooldownAndSuccessfulWorkResetsIt() throws Exception {
    var relay = mock(OutboxRelay.class);
    var wakeup = mock(RelayWakeup.class);
    var failed = new OutboxRelay.BatchResult(1, 0, 0, 0, 0, OutboxRelay.Outcome.TRANSACTION_FAILED);
    when(wakeup.isOpen()).thenReturn(true);
    when(relay.runBatch()).thenReturn(failed, failed, failed, batch(1), failed);
    when(wakeup.awaitCooldown(org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(true, true, true, false);
    when(wakeup.awaitSignal(org.mockito.ArgumentMatchers.anyLong())).thenReturn(true);
    var config =
        BridgeConfig.from(
            Map.of("poll-ms", "100", "retry-initial-ms", "1000", "retry-max-ms", "4000"));
    new RelayWorker(relay, wakeup, config).run();
    var delay = org.mockito.ArgumentCaptor.forClass(Long.class);
    verify(wakeup, times(4)).awaitCooldown(delay.capture());
    long[] ceilings = {1000, 2000, 4000, 1000};
    for (int i = 0; i < ceilings.length; i++) {
      assertTrue(delay.getAllValues().get(i) >= ceilings[i] / 2);
      assertTrue(delay.getAllValues().get(i) <= ceilings[i]);
    }
  }

  @Test
  void capturesDuringFailingScansCannotBypassTheMinimumCooldown() {
    var wakeup = new RelayWakeup();
    var scans = new java.util.concurrent.atomic.AtomicInteger();
    var config =
        BridgeConfig.from(Map.of("poll-ms", "30", "retry-initial-ms", "1", "retry-max-ms", "1"));
    var relay =
        new OutboxRelay(
            work -> {
              if (scans.incrementAndGet() == 3) {
                wakeup.close();
              } else {
                wakeup.signal();
              }
              throw new IllegalStateException("injected database failure");
            },
            null,
            config);
    long started = System.nanoTime();
    org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
        java.time.Duration.ofSeconds(3), () -> new RelayWorker(relay, wakeup, config).run());
    assertTrue(
        System.nanoTime() - started >= java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(60));
  }
}
