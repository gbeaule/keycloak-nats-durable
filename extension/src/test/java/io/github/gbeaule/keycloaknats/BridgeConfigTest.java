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
  void backoffIsBoundedAndDoesNotOverflow() {
    var config = BridgeConfig.from(Map.of());
    for (long attempts : new long[] {0, 1, 5, 63, Long.MAX_VALUE}) {
      for (int i = 0; i < 100; i++) {
        long delay = RetryBackoff.delay(config, attempts);
        assertTrue(delay >= 500 && delay <= 60000);
        if (attempts >= 63) {
          assertTrue(delay >= 30000);
        }
      }
    }
  }
}
