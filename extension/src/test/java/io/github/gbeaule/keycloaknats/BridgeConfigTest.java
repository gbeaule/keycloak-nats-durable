package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BridgeConfigTest {
  @Test
  void defaultsRequireReplicatedStorage() {
    var c = BridgeConfig.from(Map.of());
    assertEquals(3, c.minReplicas());
    assertEquals("KEYCLOAK_EVENTS", c.stream());
    assertEquals(65536, c.maxPayloadBytes());
    assertEquals(1, c.relayWorkers());
    assertFalse(c.tls().enabled());
    assertEquals("nats://localhost:4222", c.servers()[0]);
  }

  @ParameterizedTest
  @CsvSource({
    "nats-url,http://host:4222",
    "nats-url,nats://user:password@host:4222",
    "nats-url,nats://host/path",
    "nats-url,nats://host?token=secret",
    "nats-url,nats://host:99999",
    "nats-url,nats://host:0",
    "nats-url,'nats://host:4222,'",
    "nats-url,',nats://host:4222'",
    "subject-prefix,events.*",
    "subject-prefix,events..x",
    "stream,foo.bar",
    "min-replicas,0",
    "min-replicas,6",
    "batch-size,0",
    "batch-size,1001",
    "relay-workers,0",
    "relay-workers,17",
    "max-payload-bytes,1",
    "max-payload-bytes,1048577",
    "poll-ms,0",
    "idle-poll-max-ms,499",
    "timeout-ms,-1",
    "retry-max-ms,500",
    "batch-size,wat",
    "filter-reload-ms,99",
    "filter-reload-ms,60001",
    "tls-ca-file,/certs/ca.pem",
    "tls-key-file,/certs/key.pem",
    "nats-url,'tls://host:4222,nats://backup:4222'"
  })
  void invalidSettingsAreRejected(String key, String value) {
    assertThrows(IllegalArgumentException.class, () -> BridgeConfig.from(Map.of(key, value)));
  }

  @Test
  void neverPrintsSecretsAndDefensivelyCopiesServers() {
    var config =
        BridgeConfig.from(
            Map.of("token", "secret-value", "nats-url", "tls://nats:4222,tls://backup:4222"));
    assertFalse(config.toString().contains("secret-value"));
    config.servers()[0] = "changed";
    assertEquals("tls://nats:4222", config.servers()[0]);
    assertThrows(
        IllegalArgumentException.class,
        () -> BridgeConfig.from(Map.of("token", "secret", "credentials-file", "file")));
  }

  @Test
  void authenticationTokensRetainTheirExactValue() {
    var config = BridgeConfig.from(Map.of("token", " private-token "));
    assertEquals(" private-token ", config.token());
    assertFalse(config.toString().contains("private-token"));
  }

  @ParameterizedTest
  @CsvSource({
    "-1,500,1000",
    "0,500,1000",
    "1,1000,2000",
    "5,16000,32000",
    "6,30000,60000",
    "63,30000,60000",
    "9223372036854775807,30000,60000"
  })
  void backoffGrowsPerAttemptAndSaturatesWithoutOverflow(
      long attempts, long minimum, long maximum) {
    var config = BridgeConfig.from(Map.of());
    for (int i = 0; i < 50; i++) {
      long delay = RetryBackoff.delay(config, attempts);
      assertTrue(delay >= minimum && delay <= maximum, "Unexpected retry delay: " + delay);
    }
  }

  @Test
  void minimalRetryIntervalRemainsPositiveWithoutRequiringRandomVariation() {
    var config = BridgeConfig.from(Map.of("retry-initial-ms", "1", "retry-max-ms", "1"));
    assertEquals(1, RetryBackoff.delay(config, 0));
    assertEquals(1, RetryBackoff.delay(config, Long.MAX_VALUE));
  }
}
