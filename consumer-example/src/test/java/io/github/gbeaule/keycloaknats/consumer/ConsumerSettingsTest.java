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
import org.junit.jupiter.params.provider.CsvSource;
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

  @ParameterizedTest
  @CsvSource({
    "KND_CONSUMER_ACK_WAIT_MS,1000,3600000",
    "KND_CONSUMER_ACK_TIMEOUT_MS,1,60000",
    "KND_CONSUMER_PROGRESS_MS,1,300000",
    "KND_CONSUMER_MAX_ACK_PENDING,1,1000000",
    "KND_CONSUMER_MONITOR_SECONDS,1,3600",
    "KND_CONSUMER_HEALTH_PORT,0,65535"
  })
  void numericSettingsAcceptBothLimitsAndRejectValuesOutsideThem(String name, int min, int max) {
    var values =
        new java.util.HashMap<>(
            Map.of("KND_CONSUMER_ACK_WAIT_MS", "3600000", "KND_CONSUMER_PROGRESS_MS", "1"));
    for (int valid : new int[] {min, max}) {
      values.put(name, Integer.toString(valid));
      assertDoesNotThrow(() -> ConsumerSettings.from(values::get));
    }
    for (int invalid : new int[] {min - 1, max + 1}) {
      values.put(name, Integer.toString(invalid));
      assertThrows(IllegalArgumentException.class, () -> ConsumerSettings.from(values::get));
    }
  }

  @Test
  void progressRequiresStrictlyMoreThanTwiceItsIntervalLocallyAndOnTheBroker() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ConsumerSettings.from(Map.of("KND_CONSUMER_ACK_WAIT_MS", "10000")::get));
    assertDoesNotThrow(
        () -> ConsumerSettings.from(Map.of("KND_CONSUMER_ACK_WAIT_MS", "10001")::get));
    var settings = ConsumerSettings.from(name -> null);
    var exact = safe().ackWait(Duration.ofMillis(10000)).build();
    var above = safe().ackWait(Duration.ofMillis(10001)).build();
    assertThrows(IllegalStateException.class, () -> settings.validateProgress(exact));
    assertDoesNotThrow(() -> settings.validateProgress(above));
    var backoff =
        safe()
            .ackWait(Duration.ofMinutes(1))
            .backoff(Duration.ofMinutes(1), Duration.ofMillis(10000))
            .build();
    assertThrows(IllegalStateException.class, () -> settings.validateProgress(backoff));
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
    var configuration = builder.build();
    assertThrows(IllegalStateException.class, () -> ConsumerMain.validateConsumer(configuration));
  }
}
