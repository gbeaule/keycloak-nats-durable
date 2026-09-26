package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RelayWorkerTest {
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
              return 0;
            })
        .doAnswer(
            call -> {
              recovered.countDown();
              wakeup.close();
              return 1;
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
    doAnswer(call -> config.batchSize())
        .doAnswer(
            call -> {
              wakeup.close();
              return 0;
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
}
