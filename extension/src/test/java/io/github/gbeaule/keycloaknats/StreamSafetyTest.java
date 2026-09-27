package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class StreamSafetyTest {
  private final BridgeConfig config = BridgeConfig.from(Map.of());

  static StreamConfiguration.Builder safe() {
    return StreamConfiguration.builder()
        .name("KEYCLOAK_EVENTS")
        .subjects("keycloak.events.>")
        .storageType(StorageType.File)
        .replicas(3)
        .retentionPolicy(RetentionPolicy.WorkQueue)
        .discardPolicy(DiscardPolicy.New)
        .duplicateWindow(Duration.ofMinutes(2));
  }

  @Test
  void acceptsWorkQueueAndLimitsWithSafeSettings() {
    assertDoesNotThrow(() -> StreamSafety.validate(safe().build(), config));
    assertDoesNotThrow(
        () ->
            StreamSafety.validate(safe().retentionPolicy(RetentionPolicy.Limits).build(), config));
  }

  static Stream<Consumer<StreamConfiguration.Builder>> unsafe() {
    return Stream.of(
        b -> b.storageType(StorageType.Memory),
        b -> b.replicas(1),
        b -> b.retentionPolicy(RetentionPolicy.Interest),
        b -> b.discardPolicy(DiscardPolicy.Old),
        b -> b.maxAge(Duration.ofDays(1)),
        b -> b.maxMessagesPerSubject(1),
        b -> b.noAck(true),
        b -> b.allowRollup(true),
        b -> b.allowMessageTtl(),
        b -> b.maximumMessageSize(1024),
        b -> b.subjects("other.>"),
        b -> b.name("WRONG"));
  }

  @ParameterizedTest
  @MethodSource("unsafe")
  void rejectsLossyOrMisroutedStreams(Consumer<StreamConfiguration.Builder> mutate) {
    var builder = safe();
    mutate.accept(builder);
    var stream = builder.build();
    var failure =
        assertThrows(UnsafeStreamException.class, () -> StreamSafety.validate(stream, config));
    assertNull(failure.getCause());
  }

  @Test
  void usesTheConfiguredDestinationReplicaCountAndPayloadBudget() {
    var custom =
        BridgeConfig.from(
            Map.of(
                "stream",
                "CUSTOM",
                "subject-prefix",
                "auth",
                "min-replicas",
                "1",
                "max-payload-bytes",
                "1024"));
    var stream =
        safe().name("CUSTOM").subjects("auth.>").replicas(1).maximumMessageSize(1536).build();
    assertDoesNotThrow(() -> StreamSafety.validate(stream, custom));
    var tooSmall = StreamConfiguration.builder(stream).maximumMessageSize(1535).build();
    var failure =
        assertThrows(UnsafeStreamException.class, () -> StreamSafety.validate(tooSmall, custom));
    assertEquals(
        "Stream message size must leave room for the payload and headers", failure.getMessage());
    assertNull(failure.getCause());
  }
}
