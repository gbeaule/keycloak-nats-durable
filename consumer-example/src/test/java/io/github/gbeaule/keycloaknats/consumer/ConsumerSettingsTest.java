package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;

class ConsumerSettingsTest {
  @Test
  void defaultsBoundJdbcAndRequireExplicitMigrations() {
    var settings = ConsumerSettings.from(name -> null);
    assertFalse(settings.autoMigrate());
    var database = new PGSimpleDataSource();
    database.setURL("jdbc:postgresql://localhost/test?socketTimeout=0&connectTimeout=0");
    settings.processing().configure(database);
    assertEquals(5, database.getConnectTimeout());
    assertEquals(5, database.getLoginTimeout());
    assertEquals(15, database.getSocketTimeout());
    assertEquals(10, database.getQueryTimeout());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "KND_CONSUMER_CONNECT_SECONDS",
        "KND_CONSUMER_SOCKET_SECONDS",
        "KND_CONSUMER_STATEMENT_MS",
        "KND_CONSUMER_LOCK_MS",
        "KND_CONSUMER_DEADLINE_MS",
        "KND_CONSUMER_CLEANUP_MS",
        "KND_CONSUMER_PROGRESS_MS",
        "KND_CONSUMER_ACK_TIMEOUT_MS"
      })
  void deadlinesCannotBeSilentlyDisabled(String name) {
    assertThrows(
        IllegalArgumentException.class, () -> ConsumerSettings.from(Map.of(name, "0")::get));
  }

  @Test
  void progressUsesTheLivePolicyIncludingBackoff() {
    var settings = ConsumerSettings.from(name -> null);
    settings.validateProgress(safe().ackWait(Duration.ofSeconds(30)).build());
    assertThrows(
        IllegalStateException.class,
        () -> settings.validateProgress(safe().ackWait(Duration.ofSeconds(5)).build()));
    assertThrows(
        IllegalStateException.class,
        () ->
            settings.validateProgress(
                safe().ackWait(Duration.ofMinutes(2)).backoff(Duration.ofSeconds(2)).build()));
  }

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
