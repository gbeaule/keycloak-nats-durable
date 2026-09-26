package io.github.gbeaule.keycloaknats.consumer;

import java.util.concurrent.atomic.AtomicLong;

/** Monotonic attempt count that cannot wrap into negative values in a long-running process. */
final class SaturatingCounter {
  private final AtomicLong value;

  SaturatingCounter(long initial) {
    if (initial < 0) {
      throw new IllegalArgumentException("Counter must be nonnegative");
    }
    value = new AtomicLong(initial);
  }

  void increment() {
    value.getAndUpdate(current -> current == Long.MAX_VALUE ? current : current + 1);
  }

  long value() {
    return value.get();
  }
}
