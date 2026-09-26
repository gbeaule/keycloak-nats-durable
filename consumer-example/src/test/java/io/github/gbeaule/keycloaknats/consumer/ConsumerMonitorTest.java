package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ConsumerMonitorTest {
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
