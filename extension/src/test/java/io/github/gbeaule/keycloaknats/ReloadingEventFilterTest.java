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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
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
    try (var logs = new LogCapture(ReloadingEventFilter.class);
        var filter = new ReloadingEventFilter(file.toString())) {
      EventFilter original = filter.current();
      filter.reload();
      assertSame(original, filter.current());
      assertEquals(
          1, logs.records().size(), "An unchanged valid file must not produce repeated logs");
      Files.delete(file);
      filter.reload();
      assertEquals(2, logs.records().size(), "The first failure must be reported immediately");
      filter.reload();
      assertSame(original, filter.current());
      assertEquals(2, logs.records().size(), "Repeated failures must log only the first rejection");
      assertEquals(Level.SEVERE, logs.records().get(1).getLevel());
      Files.writeString(file, policy);
      filter.reload();
      assertSame(original, filter.current());
      assertEquals(3, logs.records().size());
      assertEquals(
          "Event filter is readable again; active policy unchanged",
          logs.records().get(2).getMessage());
      filter.reload();
      assertEquals(3, logs.records().size(), "Recovery must only be logged once");
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

  @Test
  void heldCallbackCannotMixCaptureAndDeliveryAcrossConcurrentReload() throws Exception {
    Path file = directory.resolve("filter.json");
    String first =
        DeliveryRulesTest.document(
            DeliveryRulesTest.rule(
                "first",
                DeliveryRulesTest.ADMIN_MATCH,
                "{\"action\":\"discard\",\"maxFailures\":2}"));
    Files.writeString(file, first);
    var acquired = new CountDownLatch(1);
    var reloaded = new CountDownLatch(1);
    try (var filter = new ReloadingEventFilter(file.toString());
        var callbacks = Executors.newSingleThreadExecutor()) {
      String digest = filter.current().sha256();
      var callback =
          callbacks.submit(
              () -> {
                EventFilter snapshot = filter.current();
                var event = EventEnvelopeTest.admin(OperationType.UPDATE);
                assertTrue(snapshot.mayAccept(event, "keycloak.events.realm.admin.user.update"));
                acquired.countDown();
                assertTrue(reloaded.await(10, TimeUnit.SECONDS));
                return DeliveryRulesTest.resolve(snapshot, event, false);
              });
      try {
        assertTrue(acquired.await(10, TimeUnit.SECONDS));
        Files.writeString(
            file,
            first
                .replace("\"operations\":[\"*\"]", "\"operations\":[]")
                .replace("\"first\"", "\"second\"")
                .replace("\"maxFailures\":2", "\"maxFailures\":3"));
        filter.reload();
      } finally {
        reloaded.countDown();
      }
      var resolved = callback.get(10, TimeUnit.SECONDS);
      assertEquals("first", resolved.ruleId());
      assertEquals(digest, resolved.filterSha256());
      assertEquals(new PublicationPolicy(null, 2), resolved.policy());
      var second = filter.current();
      var event = EventEnvelopeTest.admin(OperationType.UPDATE);
      assertTrue(
          second
              .resolve(
                  event,
                  AffectedUser.resolve(event),
                  false,
                  "keycloak.events.realm.admin.user.update")
              .isEmpty());
      // A valid capture change plus invalid delivery policy cannot partially replace the snapshot.
      Files.writeString(file, first.replace("\"maxFailures\":2", "\"maxFailures\":0"));
      filter.reload();
      assertSame(second, filter.current());
    }
  }

  @Test
  void invalidInitialDeliveryPolicyFailsStartup() throws Exception {
    Path file = directory.resolve("filter.json");
    Files.writeString(
        file,
        DeliveryRulesTest.document(
            DeliveryRulesTest.rule(
                "bad", DeliveryRulesTest.USER_MATCH, "{\"action\":\"discard\"}")));
    assertThrows(IllegalArgumentException.class, () -> new ReloadingEventFilter(file.toString()));
  }
}
