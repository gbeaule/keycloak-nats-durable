package io.github.gbeaule.keycloaknats.jetstream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.Mirror;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.Source;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.SubjectTransform;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class StreamPolicyTest {
  private static final String MODES = "Rollup, TTL, scheduling and counter modes must be disabled";
  private static final String LOCAL = "Stream must accept untransformed local publications";

  @Test
  void serverPayloadBudgetIncludesHeadersWithoutIntegerOverflow() {
    assertDoesNotThrow(() -> StreamPolicy.validateServerPayload(1536, 1024));
    assertThrows(
        StreamPolicy.Violation.class, () -> StreamPolicy.validateServerPayload(1535, 1024));
    assertThrows(StreamPolicy.Violation.class, () -> StreamPolicy.validateServerPayload(0, 1024));
    assertThrows(
        StreamPolicy.Violation.class,
        () -> StreamPolicy.validateServerPayload(Integer.MAX_VALUE, Integer.MAX_VALUE));
  }

  @ParameterizedTest
  @EnumSource(
      value = RetentionPolicy.class,
      names = {"WorkQueue", "Limits"})
  void acceptsDurableStreamsWithBackpressureAndTheRequiredHeaderBudget(RetentionPolicy retention) {
    var stream = safe().retentionPolicy(retention).maximumMessageSize(1536).build();
    assertDoesNotThrow(() -> StreamPolicy.validate(stream, "EVENTS", "auth.events", 3, 1024));
    var bounded = StreamConfiguration.builder(stream).maxMessages(10).maxBytes(100000).build();
    assertDoesNotThrow(() -> StreamPolicy.validate(bounded, "EVENTS", "auth.events", 3, 1024));
  }

  @Test
  void acceptsUnlimitedMessageSizeAndMoreReplicasThanRequired() {
    var stream = safe().replicas(5).maximumMessageSize(-1).build();
    assertDoesNotThrow(() -> StreamPolicy.validate(stream, "EVENTS", "auth.events", 3, 1024));
  }

  @Test
  void validatesTheRequestedDestinationAndReplicaCountRatherThanHardcodedDefaults() {
    var stream = safe().name("CUSTOM").subjects("custom.subject.>").replicas(1).build();
    assertDoesNotThrow(() -> StreamPolicy.validate(stream, "CUSTOM", "custom.subject", 1, 1024));
    assertThrows(
        StreamPolicy.Violation.class,
        () -> StreamPolicy.validate(stream, "CUSTOM", "custom.subject", 2, 1024));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unsafeStreams")
  void rejectsEachUnsafeSettingWithSafeReason(
      String name, Consumer<StreamConfiguration.Builder> change, String reason) {
    var builder = safe();
    change.accept(builder);
    // Build outside assertThrows so a jnats validation exception cannot satisfy the assertion.
    var stream = builder.build();
    var failure =
        assertThrows(
            StreamPolicy.Violation.class,
            () -> StreamPolicy.validate(stream, "EVENTS", "auth.events", 3, 1024),
            name);
    assertEquals(reason, failure.getMessage());
    assertNull(failure.getCause());
  }

  static Stream<Arguments> unsafeStreams() {
    return Stream.of(
        unsafe("wrong stream", "Unexpected stream", b -> b.name("SECRET")),
        unsafe(
            "wrong prefix",
            "Stream must own the configured subject prefix exclusively",
            b -> b.subjects("other.>")),
        unsafe(
            "additional subjects",
            "Stream must own the configured subject prefix exclusively",
            b -> b.subjects("auth.events.>", "other.>")),
        unsafe(
            "memory storage", "File storage is required", b -> b.storageType(StorageType.Memory)),
        unsafe("insufficient replicas", "Insufficient stream replicas", b -> b.replicas(2)),
        unsafe(
            "interest retention",
            "Interest retention can lose events without consumers",
            b -> b.retentionPolicy(RetentionPolicy.Interest)),
        unsafe("eviction", "DiscardNew is required", b -> b.discardPolicy(DiscardPolicy.Old)),
        unsafe("expiry", "Stream expiry must be disabled", b -> b.maxAge(Duration.ofNanos(1))),
        unsafe(
            "per-subject limit",
            "Per-subject limits must be disabled",
            b -> b.maxMessagesPerSubject(1)),
        unsafe("no publish ACK", "Publish acknowledgements are required", b -> b.noAck(true)),
        unsafe("rollup", MODES, b -> b.allowRollup(true)),
        unsafe("TTL", MODES, b -> b.allowMessageTtl()),
        unsafe("schedules", MODES, b -> b.allowMessageSchedules()),
        unsafe("counter", MODES, b -> b.allowMessageCounter()),
        unsafe("mirror", LOCAL, b -> b.mirror(Mirror.builder().name("OTHER").build())),
        unsafe(
            "transform",
            LOCAL,
            b -> b.subjectTransform(new SubjectTransform("auth.events.>", "other.>"))),
        unsafe("sources", LOCAL, b -> b.addSource(Source.builder().name("OTHER").build())),
        unsafe(
            "no deduplication",
            "A deduplication window is required",
            b -> b.duplicateWindow(Duration.ZERO)),
        unsafe(
            "header budget short by one byte",
            "Stream message size must leave room for the payload and headers",
            b -> b.maximumMessageSize(1535)));
  }

  @Test
  void rejectsSealedStreamReturnedByTheServer() throws Exception {
    var stream =
        StreamConfiguration.instance(
            """
            {"name":"EVENTS","subjects":["auth.events.>"],"storage":"file","num_replicas":3,
             "retention":"workqueue","discard":"new","duplicate_window":120000000000,
             "max_msgs_per_subject":-1,"max_msg_size":-1,"sealed":true}
            """);
    var failure =
        assertThrows(
            StreamPolicy.Violation.class,
            () -> StreamPolicy.validate(stream, "EVENTS", "auth.events", 3, 1024));
    assertEquals(LOCAL, failure.getMessage());
  }

  @Test
  void payloadHeaderArithmeticCannotOverflowIntoAcceptingAnUndersizedStream() {
    var stream = safe().maximumMessageSize(Integer.MAX_VALUE).build();
    assertThrows(
        StreamPolicy.Violation.class,
        () -> StreamPolicy.validate(stream, "EVENTS", "auth.events", 3, Integer.MAX_VALUE));
  }

  @ParameterizedTest
  @CsvSource({
    "max_msgs_per_subject,Max Messages Per Subject must be greater than zero or -1 for unlimited",
    "max_msg_size,Max Message Size must be greater than zero or -1 for unlimited"
  })
  void clientRejectsZeroLimitsBeforeTheyCanReachTheStreamPolicy(String field, String reason)
      throws Exception {
    // The client rejects zero; -1 is the only representation of an unlimited limit.
    String response =
        """
            {"name":"EVENTS","subjects":["auth.events.>"],"storage":"file","num_replicas":3,
             "retention":"workqueue","discard":"new","duplicate_window":120000000000,"%s":0}
        """;
    var failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> StreamConfiguration.instance(response.formatted(field)));
    assertEquals(reason, failure.getMessage());
  }

  private static Arguments unsafe(
      String name, String reason, Consumer<StreamConfiguration.Builder> change) {
    return Arguments.of(name, change, reason);
  }

  private static StreamConfiguration.Builder safe() {
    return StreamConfiguration.builder()
        .name("EVENTS")
        .subjects("auth.events.>")
        .storageType(StorageType.File)
        .replicas(3)
        .retentionPolicy(RetentionPolicy.WorkQueue)
        .discardPolicy(DiscardPolicy.New)
        .duplicateWindow(Duration.ofMinutes(2));
  }
}
