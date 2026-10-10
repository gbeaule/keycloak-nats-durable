package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BenchmarkTimeoutsTest {
  private static final List<String> SETTINGS =
      List.of(
          "benchmark.workload-timeout-seconds",
          "benchmark.drain-timeout-seconds",
          "benchmark.recovery-timeout-seconds");
  private final Map<String, String> previous = new HashMap<>();

  @BeforeEach
  void resetProperties() {
    for (String name : SETTINGS) {
      previous.put(name, System.getProperty(name));
      System.clearProperty(name);
    }
  }

  @AfterEach
  void restoreProperties() {
    previous.forEach(
        (name, value) -> {
          if (value == null) {
            System.clearProperty(name);
          } else {
            System.setProperty(name, value);
          }
        });
  }

  @Test
  void ordinaryAndSourceDrainDefaultsRemainSeparateFromWorkloadBudgets() {
    assertEquals(
        Map.of("workloadSeconds", 300L, "drainSeconds", 20L, "recoverySeconds", 300L),
        BenchmarkTimeouts.configured(20).report());
    assertEquals(Duration.ofSeconds(300), BenchmarkTimeouts.configured(300).drain());
  }

  @Test
  void explicitLargeWorkloadBudgetsAreRecorded() {
    for (String name : SETTINGS) {
      System.setProperty(name, "1200");
    }
    assertEquals(
        Map.of("workloadSeconds", 1200L, "drainSeconds", 1200L, "recoverySeconds", 1200L),
        BenchmarkTimeouts.configured(20).report());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "86401", "2147483648", "invalid"})
  void invalidWaitBudgetsFailBeforeTheWorkloadStarts(String value) {
    for (String name : SETTINGS) {
      System.setProperty(name, value);
      assertThrows(IllegalArgumentException.class, () -> BenchmarkTimeouts.configured(20));
      System.clearProperty(name);
    }
  }

  @Test
  void expiredPhaseBudgetCancelsUnfinishedWork() {
    var pending = new CompletableFuture<Void>();
    assertThrows(
        TimeoutException.class,
        () -> BenchmarkTimeouts.finish(List.of(pending), 0, Duration.ofNanos(10), () -> 11));
    assertTrue(pending.isCancelled());
  }

  @Test
  void workloadFailuresPropagateAndCancelOtherQueuedWork() {
    var failed = CompletableFuture.failedFuture(new IllegalStateException("Capture failed"));
    var pending = new CompletableFuture<Void>();
    var failure =
        assertThrows(
            ExecutionException.class,
            () ->
                BenchmarkTimeouts.finish(
                    List.of(failed, pending), 0, Duration.ofNanos(10), () -> 0));
    assertEquals("Capture failed", failure.getCause().getMessage());
    assertTrue(pending.isCancelled());
  }

  @Test
  void queuedFuturesShareOneDeadline() throws Exception {
    var now = new AtomicLong();
    Future<?> first = mock(Future.class);
    Future<?> second = mock(Future.class);
    when(first.isDone()).thenReturn(true);
    when(second.isDone()).thenReturn(true);
    when(first.get(anyLong(), eq(TimeUnit.NANOSECONDS)))
        .thenAnswer(
            call -> {
              assertEquals(100L, call.getArgument(0, Long.class));
              now.set(60);
              return null;
            });
    when(second.get(anyLong(), eq(TimeUnit.NANOSECONDS)))
        .thenAnswer(
            call -> {
              assertEquals(40L, call.getArgument(0, Long.class));
              return null;
            });
    BenchmarkTimeouts.finish(List.of(first, second), 0, Duration.ofNanos(100), now::get);
  }
}
