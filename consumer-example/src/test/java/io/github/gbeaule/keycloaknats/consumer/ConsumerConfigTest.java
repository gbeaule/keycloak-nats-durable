package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConsumerConfigTest {
  @Test
  void overridesReachTransportDatabaseAndWorkerWithoutLeakingSecrets() {
    var config =
        new ConsumerConfig(
            Map.of(
                    "KND_NATS_URL",
                    "nats://broker:4222",
                    "KND_TOKEN",
                    " private-token ",
                    "KND_CONSUMER_DB_URL",
                    "jdbc:postgresql://database/app?socketTimeout=0",
                    "KND_CONSUMER_DB_PASSWORD",
                    " private-password ",
                    "KND_CONSUMER_SOCKET_SECONDS",
                    "12",
                    "KND_CONSUMER_DB_POOL_SIZE",
                    "2",
                    "KND_CONSUMER_DEADLINE_MS",
                    "5000")
                ::get);
    var settings = config.settings();
    var database = config.database(settings.processing());
    assertEquals(12, database.getSocketTimeout());
    assertEquals(" private-password ", database.getPassword());
    assertEquals(5000, settings.processing().deadlineMs());
    assertEquals(2, config.poolSize());
    assertEquals("nats://broker:4222", config.nats().servers()[0]);
    assertEquals(" private-token ", config.nats().token());
    assertFalse(config.nats().toString().contains("private-token"));
  }

  @Test
  void transportNormalizesAddressesAndDefensivelyCopiesThem() {
    var config =
        new ConsumerConfig(Map.of("KND_NATS_URL", " tls://broker:4222 , tls://backup:4222 ")::get);
    var transport = config.nats();
    assertArrayEquals(new String[] {"tls://broker:4222", "tls://backup:4222"}, transport.servers());
    transport.servers()[0] = "nats://changed";
    assertEquals("tls://broker:4222", transport.servers()[0]);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " ",
        "nats://broker:4222,",
        ",nats://broker:4222",
        "nats://broker:4222,,nats://backup:4222",
        "http://broker:4222",
        "nats://user:secret@broker:4222",
        "nats://broker:4222/path",
        "nats://broker?token=secret",
        "nats://broker#secret",
        "nats://broker:0",
        "nats://broker:99999",
        "nats://secret@[",
        "tls://broker:4222,nats://backup:4222"
      })
  void unsafeTransportAddressesFailWithoutEchoingCredentials(String servers) {
    var config = new ConsumerConfig(Map.of("KND_NATS_URL", servers)::get);
    var failure = assertThrows(IllegalArgumentException.class, config::nats);
    assertFalse(failure.toString().contains("secret"));
    assertEquals(null, failure.getCause());
  }

  @Test
  void collectorUsesItsOwnSettingsWithoutLoadingNatsOrWorkerPolicies() {
    var config =
        new ConsumerConfig(
            Map.of(
                    "KND_OUTBOX_DB_URL",
                    "jdbc:postgresql://database/keycloak",
                    "KND_REPORT_REALM_ID",
                    "realm/a",
                    "KND_SUBJECT_PREFIX",
                    "auth",
                    "KND_REPORT_MAX_ROWS",
                    "42",
                    "KND_REPORT_TIMEOUT_SECONDS",
                    "3",
                    "KND_TOKEN",
                    "token",
                    "KND_CREDENTIALS_FILE",
                    "conflicting.creds",
                    "KND_CONSUMER_DEADLINE_MS",
                    "invalid")
                ::get);
    var report = config.report();
    assertEquals("auth.cmVhbG0vYQ.", report.realmPrefix());
    assertEquals(42, report.maxRows());
    assertEquals(3, report.database().getSocketTimeout());
    assertThrows(IllegalArgumentException.class, config::nats);
  }

  @Test
  void invalidNumericInputsFailWithoutEchoingTheirValues() {
    var config = new ConsumerConfig(Map.of("KND_CONSUMER_DB_POOL_SIZE", "secret")::get);
    var failure = assertThrows(IllegalArgumentException.class, config::poolSize);
    assertFalse(failure.getMessage().contains("secret"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new ConsumerConfig(Map.of("KND_CONSUMER_DB_POOL_SIZE", "2147483647")::get).poolSize());
  }
}
