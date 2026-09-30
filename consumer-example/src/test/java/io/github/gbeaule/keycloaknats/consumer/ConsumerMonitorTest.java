package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ConsumerMonitorTest {
  @Test
  void readinessTracksConnectionFailureAndShutdown() throws Exception {
    var connected = new AtomicBoolean(true);
    var settings = ConsumerSettings.from(name -> null);
    try (var monitor = new ConsumerMonitor(settings, connected::get)) {
      assertTrue(monitor.ready());
      connected.set(false);
      assertFalse(monitor.ready());
      assertEquals(0, metric(monitor, "ready"));
      connected.set(true);
      assertTrue(monitor.ready());
      monitor.failed();
      assertFalse(monitor.ready());
    }
    var closed = new ConsumerMonitor(settings, () -> true);
    closed.close();
    assertFalse(closed.ready());
  }

  @Test
  void overdueWorkMakesTheWorkerUnreadyUntilThatAttemptEnds() throws Exception {
    var settings =
        ConsumerSettings.from(
            Map.of(
                    "KND_CONSUMER_DEADLINE_MS",
                    "10",
                    "KND_CONSUMER_CLEANUP_MS",
                    "10",
                    "KND_CONSUMER_ACK_TIMEOUT_MS",
                    "10")
                ::get);
    try (var monitor = new ConsumerMonitor(settings, () -> true)) {
      assertTrue(monitor.ready());
      final var attempt = monitor.begin();
      TimeUnit.MILLISECONDS.sleep(100);
      assertFalse(monitor.ready());
      double age = metric(monitor, "oldest_processing_seconds");
      assertTrue(age >= 0.05 && age < 30, "Age must be elapsed seconds");
      monitor.end(attempt);
      assertTrue(monitor.ready());
      assertEquals(0, metric(monitor, "oldest_processing_seconds"));
    }
  }

  @Test
  void onlyCommittedOrDuplicateEventsAdvanceTheLastCommitTimestamp() throws Exception {
    for (var counter : ConsumerMonitor.Counter.values()) {
      try (var monitor = new ConsumerMonitor(ConsumerSettings.from(name -> null), () -> true)) {
        long before = Instant.now().getEpochSecond();
        monitor.increment(counter);
        double timestamp = metric(monitor, "last_commit_timestamp_seconds");
        if (counter == ConsumerMonitor.Counter.COMMITTED
            || counter == ConsumerMonitor.Counter.DUPLICATES) {
          assertTrue(timestamp >= before && timestamp <= Instant.now().getEpochSecond());
        } else {
          assertEquals(0, timestamp, counter.name());
        }
        assertEquals(1, monitor.count(counter));
      }
    }
  }

  private static double metric(ConsumerMonitor monitor, String name) {
    String prefix = "knd_consumer_" + name + " ";
    return monitor
        .metrics()
        .lines()
        .filter(line -> line.startsWith(prefix))
        .mapToDouble(line -> Double.parseDouble(line.substring(prefix.length())))
        .findFirst()
        .orElseThrow();
  }

  @Test
  void concurrentIncrementsSaturateAtStorageLimit() throws Exception {
    var counter = new SaturatingCounter(Long.MAX_VALUE - 10);
    try (var workers = Executors.newFixedThreadPool(4)) {
      for (int i = 0; i < 100; i++) {
        workers.submit(counter::increment);
      }
      workers.shutdown();
      assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertEquals(Long.MAX_VALUE, counter.value());
    counter.increment();
    assertEquals(Long.MAX_VALUE, counter.value());
  }

  @Test
  void overlappingAttemptsHaveIndependentIdentitiesAndCleanup() throws Exception {
    try (var monitor = new ConsumerMonitor(ConsumerSettings.from(name -> null), () -> true)) {
      var first = monitor.begin();
      var second = monitor.begin();
      assertNotSame(first, second);
      assertTrue(monitor.metrics().contains("knd_consumer_active 2\n"));
      monitor.end(first);
      monitor.end(first);
      assertTrue(monitor.metrics().contains("knd_consumer_active 1\n"));
      monitor.end(second);
      assertTrue(monitor.metrics().contains("knd_consumer_active 0\n"));
      assertEquals(2, monitor.count(ConsumerMonitor.Counter.RECEIVED));
    }
  }
}
