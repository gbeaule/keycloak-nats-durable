package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.keycloak.events.admin.OperationType;
import org.mockito.ArgumentCaptor;

class ReloadingEventFilterTest {
  @TempDir Path directory;

  @Test
  void invalidInitialPolicyFailsStartup() throws Exception {
    Path file = directory.resolve("filter.json");
    assertThrows(IllegalArgumentException.class, () -> new ReloadingEventFilter(file.toString()));
    Files.writeString(file, "{}");
    assertThrows(IllegalArgumentException.class, () -> new ReloadingEventFilter(file.toString()));
  }

  @Test
  void absentPolicyAllowsBothEventFamiliesWithoutPolling() {
    try (var schedulers = mockStatic(Executors.class);
        var filter = new ReloadingEventFilter(null)) {
      filter.start(100);
      assertTrue(EventFilterTest.accepts(filter.current(), EventEnvelopeTest.login()));
      assertTrue(
          EventFilterTest.accepts(
              filter.current(), EventEnvelopeTest.admin(OperationType.DELETE), null));
      schedulers.verifyNoInteractions();
    }
  }

  @Test
  void unchangedReadablePolicyKeepsItsSnapshotIncludingAfterReadFailures() throws Exception {
    Path file = directory.resolve("filter.json");
    String policy = "{\"userEvents\":[\"LOGIN\"],\"adminEvents\":[]}";
    Files.writeString(file, policy);
    try (var filter = new ReloadingEventFilter(file.toString())) {
      EventFilter original = filter.current();
      filter.reload();
      assertSame(original, filter.current());
      Files.delete(file);
      filter.reload();
      filter.reload();
      assertSame(original, filter.current());
      Files.writeString(file, policy);
      filter.reload();
      assertSame(original, filter.current());
      assertTrue(EventFilterTest.accepts(filter.current(), EventEnvelopeTest.login()));
    }
  }

  @Test
  void fileSizeLimitIsInclusiveAndInitialFailuresDoNotExposeThePathOrCause() throws Exception {
    Path file = directory.resolve("secret-filter.json");
    String json = "{\"userEvents\":[],\"adminEvents\":[]}";
    Files.writeString(file, json + " ".repeat(65536 - json.length()));
    try (var filter = new ReloadingEventFilter(file.toString())) {
      assertFalse(EventFilterTest.accepts(filter.current(), EventEnvelopeTest.login()));
    }
    Files.writeString(file, json + " ".repeat(65537 - json.length()));
    var failure =
        assertThrows(
            IllegalArgumentException.class, () -> new ReloadingEventFilter(file.toString()));
    assertEquals("Cannot load initial event filter; check file and schema", failure.getMessage());
    assertNull(failure.getCause());
  }

  @Test
  void scheduledPollerStartsOnceReloadsPolicyAndIsStoppedOnClose() throws Exception {
    Path file = directory.resolve("filter.json");
    Files.writeString(file, "{\"userEvents\":[\"LOGIN\"],\"adminEvents\":[]}");
    var executor = mock(ScheduledExecutorService.class);
    try (var schedulers = mockStatic(Executors.class)) {
      schedulers
          .when(() -> Executors.newSingleThreadScheduledExecutor(any(ThreadFactory.class)))
          .thenReturn(executor);
      var filter = new ReloadingEventFilter(file.toString());
      try {
        filter.start(250);
        filter.start(500);
        var poll = ArgumentCaptor.forClass(Runnable.class);
        verify(executor)
            .scheduleWithFixedDelay(poll.capture(), eq(250L), eq(250L), eq(TimeUnit.MILLISECONDS));
        var threads = ArgumentCaptor.forClass(ThreadFactory.class);
        schedulers.verify(() -> Executors.newSingleThreadScheduledExecutor(threads.capture()));
        Runnable work = mock(Runnable.class);
        Thread thread = threads.getValue().newThread(work);
        assertTrue(thread.isDaemon());
        assertEquals("keycloak-nats-filter", thread.getName());
        thread.run();
        verify(work).run();
        Files.writeString(file, "{\"userEvents\":[],\"adminEvents\":[]}");
        poll.getValue().run();
        assertFalse(EventFilterTest.accepts(filter.current(), EventEnvelopeTest.login()));
      } finally {
        filter.close();
      }
      verify(executor).shutdownNow();
    }
  }

  @Test
  void unavailableDigestNeverReplacesTheLastAcceptedSnapshot() throws Exception {
    Path file = directory.resolve("filter.json");
    Files.writeString(file, "{\"userEvents\":[\"LOGIN\"],\"adminEvents\":[]}");
    try (var filter = new ReloadingEventFilter(file.toString())) {
      EventFilter original = filter.current();
      Files.writeString(file, "{\"userEvents\":[],\"adminEvents\":[]}");
      try (var digests = mockStatic(MessageDigest.class)) {
        digests
            .when(() -> MessageDigest.getInstance("SHA-256"))
            .thenThrow(new NoSuchAlgorithmException());
        filter.reload();
        assertSame(original, filter.current());
        assertTrue(EventFilterTest.accepts(filter.current(), EventEnvelopeTest.login()));
      }
      filter.reload();
      assertFalse(EventFilterTest.accepts(filter.current(), EventEnvelopeTest.login()));
    }
  }

  @Test
  void reloadsSameTimestampReplacementAndKeepsLastGoodPolicyOnEveryFailure() throws Exception {
    Path file = directory.resolve("filter.json");
    Files.writeString(file, "{\"userEvents\":[\"LOGIN\"],\"adminEvents\":[]}");
    FileTime timestamp = Files.getLastModifiedTime(file);
    try (var filter = new ReloadingEventFilter(file.toString())) {
      EventFilter first = filter.current();
      assertTrue(EventFilterTest.accepts(first, EventEnvelopeTest.login()));
      Path next = directory.resolve("next.json");
      Files.writeString(next, "{\"userEvents\":[\"LOGOUT\"],\"adminEvents\":[]}");
      Files.setLastModifiedTime(next, timestamp);
      Files.move(next, file, StandardCopyOption.REPLACE_EXISTING);
      filter.reload();
      EventFilter second = filter.current();
      assertNotSame(first, second);
      assertFalse(EventFilterTest.accepts(second, EventEnvelopeTest.login()));
      Files.writeString(file, "{\"userEvents\":[");
      filter.reload();
      assertSame(second, filter.current());
      Files.delete(file);
      filter.reload();
      assertSame(second, filter.current());
      Files.writeString(file, " ".repeat(65537));
      filter.reload();
      assertSame(second, filter.current());
      Files.writeString(file, "{\"userEvents\":[\"*\"],\"adminEvents\":[]}");
      filter.reload();
      assertTrue(EventFilterTest.accepts(filter.current(), EventEnvelopeTest.login()));
      // An in-flight callback holding the old immutable snapshot stays consistent.
      assertFalse(EventFilterTest.accepts(second, EventEnvelopeTest.login()));
    }
  }
}
