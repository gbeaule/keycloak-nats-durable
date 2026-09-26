package io.github.gbeaule.keycloaknats.consumer;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/** Bounded-cardinality worker metrics, independent of Keycloak readiness and event contents. */
public final class ConsumerMonitor implements AutoCloseable {
  /** These are attempt counters; use the database audit for unique quarantine/discard records. */
  public enum Counter {
    RECEIVED,
    COMMITTED,
    DUPLICATES,
    RETRIES,
    QUARANTINED,
    DROPPED,
    TIMEOUTS,
    ACK_FAILURES,
    PROGRESS_FAILURES
  }

  private final Map<Counter, SaturatingCounter> counters = new EnumMap<>(Counter.class);
  private final Map<Attempt, Long> active = new ConcurrentHashMap<>();
  private final AtomicLong lastCommitSeconds = new AtomicLong();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final BooleanSupplier connected;
  private final ConsumerSettings settings;
  private final ScheduledExecutorService reporter;
  private final ThreadPoolExecutor httpExecutor;
  private final HttpServer server;

  /** A zero health port disables HTTP; periodic progress reports remain enabled. */
  public ConsumerMonitor(ConsumerSettings settings, BooleanSupplier connected) throws IOException {
    this.settings = settings;
    this.connected = connected;
    for (Counter counter : Counter.values()) {
      counters.put(counter, new SaturatingCounter(0));
    }
    if (settings.healthPort() > 0) {
      server =
          HttpServer.create(new InetSocketAddress(settings.healthBind(), settings.healthPort()), 8);
      httpExecutor =
          new ThreadPoolExecutor(
              2,
              2,
              0,
              TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(16),
              Thread.ofPlatform().daemon().name("knd-monitor-http-", 0).factory(),
              new ThreadPoolExecutor.AbortPolicy());
      server.setExecutor(httpExecutor);
      server.createContext("/metrics", this::serve);
      server.createContext("/health/ready", this::serve);
      server.start();
    } else {
      server = null;
      httpExecutor = null;
    }
    reporter =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("knd-monitor").factory());
    reporter.scheduleAtFixedRate(
        () ->
            System.out.println(
                "Consumer progress: ready="
                    + ready()
                    + " active="
                    + active.size()
                    + " oldest_ms="
                    + oldestMs()
                    + " committed="
                    + count(Counter.COMMITTED)
                    + " retry="
                    + count(Counter.RETRIES)
                    + " quarantined="
                    + count(Counter.QUARANTINED)
                    + " dropped="
                    + count(Counter.DROPPED)
                    + " ack_failures="
                    + count(Counter.ACK_FAILURES)),
        settings.monitorSeconds(),
        settings.monitorSeconds(),
        TimeUnit.SECONDS);
  }

  // Object identity avoids an ever-increasing sequence and collisions after numeric wraparound.
  static final class Attempt {}

  Attempt begin() {
    increment(Counter.RECEIVED);
    var attempt = new Attempt();
    active.put(attempt, System.nanoTime());
    return attempt;
  }

  void end(Attempt attempt) {
    active.remove(attempt);
  }

  void increment(Counter counter) {
    counters.get(counter).increment();
    if (counter == Counter.COMMITTED || counter == Counter.DUPLICATES) {
      lastCommitSeconds.set(Instant.now().getEpochSecond());
    }
  }

  /** Returns a cumulative attempt count, saturating at {@link Long#MAX_VALUE}. */
  public long count(Counter counter) {
    return counters.get(counter).value();
  }

  void failed() {
    running.set(false);
  }

  /**
   * Readiness means the worker can run; delivery lag and database failures have separate metrics.
   */
  public boolean ready() {
    long bound =
        2L * (settings.processing().deadlineMs() + settings.processing().cleanupMs())
            + settings.ackTimeoutMs();
    return running.get() && connected.getAsBoolean() && oldestMs() <= bound;
  }

  /** Exports fixed metric names without user IDs, subjects, realms or payload labels. */
  public String metrics() {
    var output = new StringBuilder();
    output.append("knd_consumer_ready ").append(ready() ? 1 : 0).append('\n');
    output.append("knd_consumer_active ").append(active.size()).append('\n');
    output
        .append("knd_consumer_oldest_processing_seconds ")
        .append(oldestMs() / 1000.0)
        .append('\n');
    output
        .append("knd_consumer_last_commit_timestamp_seconds ")
        .append(lastCommitSeconds.get())
        .append('\n');
    for (Counter counter : Counter.values()) {
      output
          .append("knd_consumer_")
          .append(counter.name().toLowerCase(java.util.Locale.ROOT))
          .append("_total ")
          .append(count(counter))
          .append('\n');
    }
    return output.toString();
  }

  private long oldestMs() {
    long now = System.nanoTime();
    return active.values().stream()
        .mapToLong(start -> TimeUnit.NANOSECONDS.toMillis(now - start))
        .max()
        .orElse(0);
  }

  private void serve(HttpExchange request) throws IOException {
    try (request) {
      String path = request.getRequestURI().getPath();
      int status;
      String body;
      if (!"GET".equals(request.getRequestMethod())) {
        status = 405;
        body = "Method not allowed\n";
      } else if ("/metrics".equals(path)) {
        status = 200;
        body = metrics();
      } else if ("/health/ready".equals(path)) {
        boolean ready = ready();
        status = ready ? 200 : 503;
        body = "{\"ready\":" + ready + "}\n";
      } else {
        status = 404;
        body = "Not found\n";
      }
      request
          .getResponseHeaders()
          .set(
              "Content-Type",
              "/health/ready".equals(path) ? "application/json" : "text/plain; version=0.0.4");
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      request.sendResponseHeaders(status, bytes.length);
      request.getResponseBody().write(bytes);
    }
  }

  @Override
  public void close() {
    running.set(false);
    reporter.shutdownNow();
    if (server != null) {
      server.stop(0);
      httpExecutor.shutdownNow();
    }
  }
}
