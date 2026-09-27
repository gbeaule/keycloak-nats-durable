package io.github.gbeaule.keycloaknats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

/**
 * Loads complete snapshots off request threads, retaining the last valid policy on reload errors.
 */
final class ReloadingEventFilter implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(ReloadingEventFilter.class);
  private static final int MAX_BYTES = 65536;
  private final Path path;
  private volatile EventFilter current;
  private byte[] appliedBytes;
  private boolean failed;
  private ScheduledExecutorService executor;

  ReloadingEventFilter(String file) {
    path = file == null ? null : Path.of(file);
    if (path == null) {
      current = EventFilter.all();
    } else {
      try {
        apply(read());
      } catch (IOException | RuntimeException invalid) {
        // Fail startup, never silently broaden or disable capture on an invalid initial policy.
        throw new IllegalArgumentException(
            "Cannot load initial event filter; check file and schema");
      }
    }
  }

  EventFilter current() {
    return current;
  }

  synchronized void start(long intervalMillis) {
    if (path == null || executor != null) {
      return;
    }
    executor =
        Executors.newSingleThreadScheduledExecutor(
            work -> {
              Thread thread = new Thread(work, "keycloak-nats-filter");
              thread.setDaemon(true);
              return thread;
            });
    executor.scheduleWithFixedDelay(
        this::reload, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  synchronized void reload() {
    try {
      byte[] bytes = read();
      if (!Arrays.equals(bytes, appliedBytes)) {
        apply(bytes);
      } else if (failed) {
        logger.info("Event filter is readable again; active policy unchanged");
      }
      failed = false;
    } catch (IOException | RuntimeException invalid) {
      if (!failed) {
        logger.error(
            "Event filter reload rejected; retaining last valid policy; check file and schema");
      }
      failed = true;
    }
  }

  private byte[] read() throws IOException {
    // Resolve the path anew on each poll, including Kubernetes projected-volume symlink swaps.
    try (var input = Files.newInputStream(path)) {
      byte[] bytes = input.readNBytes(MAX_BYTES + 1);
      if (bytes.length > MAX_BYTES) {
        throw new IOException("Event filter exceeds 64 KiB");
      }
      return bytes;
    }
  }

  private void apply(byte[] bytes) throws IOException {
    EventFilter candidate = EventFilter.parse(bytes);
    current = candidate;
    appliedBytes = bytes;
    logger.infof("Event filter applied; sha256=%s", candidate.sha256());
  }

  @Override
  public synchronized void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
