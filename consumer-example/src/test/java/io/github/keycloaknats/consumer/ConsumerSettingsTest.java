package io.github.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.*;

import io.nats.client.api.*;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ConsumerSettingsTest {
  static ConsumerConfiguration.Builder safe() {
    return ConsumerConfiguration.builder()
        .durable("auth-worker")
        .ackPolicy(AckPolicy.Explicit)
        .deliverPolicy(DeliverPolicy.All)
        .maxDeliver(-1);
  }

  @Test
  void absentInactivityThresholdMeansNoExpiry() {
    var config = safe().build();
    assertNull(config.getInactiveThreshold());
    assertDoesNotThrow(() -> ConsumerMain.validateConsumer(config));
  }

  @Test
  void explicitZeroInactivityThresholdIsAlsoAccepted() {
    assertDoesNotThrow(
        () -> ConsumerMain.validateConsumer(safe().inactiveThreshold(Duration.ZERO).build()));
  }

  static Stream<Consumer<ConsumerConfiguration.Builder>> unsafe() {
    return Stream.of(
        b -> b.ackPolicy(AckPolicy.None),
        b -> b.ackPolicy(AckPolicy.All),
        b -> b.maxDeliver(5),
        b -> b.memStorage(true),
        b -> b.headersOnly(true),
        b -> b.deliverPolicy(DeliverPolicy.New),
        b -> b.inactiveThreshold(Duration.ofMinutes(5)),
        b -> b.deliverSubject("push.subject"));
  }

  @ParameterizedTest
  @MethodSource("unsafe")
  void rejectsLossyConsumers(Consumer<ConsumerConfiguration.Builder> change) {
    var builder = safe();
    change.accept(builder);
    assertThrows(IllegalStateException.class, () -> ConsumerMain.validateConsumer(builder.build()));
  }
}
