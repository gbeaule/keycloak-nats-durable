package io.github.gbeaule.keycloaknats;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

/** Whole workload budgets are separate from individual request and transaction deadlines. */
record BenchmarkTimeouts(Duration workload, Duration drain, Duration recovery) {
  static BenchmarkTimeouts configured(int defaultDrainSeconds) {
    return new BenchmarkTimeouts(
        seconds("benchmark.workload-timeout-seconds", 300),
        seconds("benchmark.drain-timeout-seconds", defaultDrainSeconds),
        seconds("benchmark.recovery-timeout-seconds", 300));
  }

  Map<String, Long> report() {
    return Map.of(
        "workloadSeconds", workload.toSeconds(),
        "drainSeconds", drain.toSeconds(),
        "recoverySeconds", recovery.toSeconds());
  }

  static void finish(Collection<? extends Future<?>> work, long began, Duration budget)
      throws Exception {
    finish(work, began, budget, System::nanoTime);
  }

  static void finish(
      Collection<? extends Future<?>> work, long began, Duration budget, LongSupplier nanoTime)
      throws Exception {
    try {
      for (var future : work) {
        long remaining = budget.toNanos() - (nanoTime.getAsLong() - began);
        if (remaining <= 0) {
          throw new TimeoutException("Benchmark workload exceeded " + budget);
        }
        future.get(remaining, TimeUnit.NANOSECONDS);
      }
    } finally {
      for (var future : work) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
  }

  private static Duration seconds(String name, int fallback) {
    int value = Integer.parseInt(System.getProperty(name, Integer.toString(fallback)));
    if (value < 1 || value > 86400) {
      throw new IllegalArgumentException(name + " must be 1..86400 seconds");
    }
    return Duration.ofSeconds(value);
  }
}
