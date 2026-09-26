package io.github.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
  void reloadsSameTimestampReplacementAndKeepsLastGoodPolicyOnEveryFailure() throws Exception {
    Path file = directory.resolve("filter.json");
    Files.writeString(file, "{\"userEvents\":[\"LOGIN\"],\"adminEvents\":[]}");
    FileTime timestamp = Files.getLastModifiedTime(file);
    try (var filter = new ReloadingEventFilter(file.toString())) {
      EventFilter first = filter.current();
      assertTrue(first.accepts(EventEnvelopeTest.login()));
      Path next = directory.resolve("next.json");
      Files.writeString(next, "{\"userEvents\":[\"LOGOUT\"],\"adminEvents\":[]}");
      Files.setLastModifiedTime(next, timestamp);
      Files.move(next, file, StandardCopyOption.REPLACE_EXISTING);
      filter.reload();
      EventFilter second = filter.current();
      assertNotSame(first, second);
      assertFalse(second.accepts(EventEnvelopeTest.login()));
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
      assertTrue(filter.current().accepts(EventEnvelopeTest.login()));
      // An in-flight callback holding the old immutable snapshot stays consistent.
      assertFalse(second.accepts(EventEnvelopeTest.login()));
    }
  }
}
