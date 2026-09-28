package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.github.gbeaule.keycloaknats.config.Environment;
import io.github.gbeaule.keycloaknats.tls.TlsConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.keycloak.Config;

class BridgeConfigTest {
  @ParameterizedTest
  @CsvSource({
    "min-replicas,1",
    "min-replicas,5",
    "batch-size,1",
    "batch-size,1000",
    "relay-workers,1",
    "relay-workers,16",
    "max-payload-bytes,1024",
    "max-payload-bytes,1048576",
    "timeout-ms,1",
    "timeout-ms,3600000",
    "poll-ms,1",
    "poll-ms,3600000",
    "idle-poll-max-ms,1",
    "idle-poll-max-ms,3600000",
    "retry-initial-ms,1",
    "retry-initial-ms,3600000",
    "retry-max-ms,1",
    "retry-max-ms,3600000",
    "filter-reload-ms,100",
    "filter-reload-ms,60000"
  })
  void configuredNumericLimitsIncludeBothEndpoints(String key, long boundary) {
    var values =
        new HashMap<>(
            Map.of(
                "poll-ms",
                "1",
                "idle-poll-max-ms",
                "3600000",
                "retry-initial-ms",
                "1",
                "retry-max-ms",
                "3600000"));
    values.put(key, Long.toString(boundary));
    var config = BridgeConfig.from(values);
    long actual =
        switch (key) {
          case "min-replicas" -> config.minReplicas();
          case "batch-size" -> config.batchSize();
          case "relay-workers" -> config.relayWorkers();
          case "max-payload-bytes" -> config.maxPayloadBytes();
          case "timeout-ms" -> config.timeout().toMillis();
          case "poll-ms" -> config.pollInterval().toMillis();
          case "idle-poll-max-ms" -> config.idlePollMax().toMillis();
          case "retry-initial-ms" -> config.retryInitial().toMillis();
          case "retry-max-ms" -> config.retryMax().toMillis();
          case "filter-reload-ms" -> config.filterReloadInterval().toMillis();
          default -> throw new AssertionError("Unexpected test setting: " + key);
        };
    assertEquals(boundary, actual);
  }

  @Test
  void destinationNamesCanUseTheFullStorageLimit() {
    String name = "a".repeat(128);
    var config = BridgeConfig.from(Map.of("stream", name, "subject-prefix", name));
    assertEquals(name, config.stream());
    assertEquals(name, config.subjectPrefix());
  }

  @Test
  void providerSettingsOverrideEnvironmentAndAbsentSettingsFallBackToIt() {
    var source =
        new Environment(
            Map.of(
                    "KND_STREAM",
                    "ENVIRONMENT",
                    "KND_TIMEOUT_MS",
                    "250",
                    "KND_TOKEN",
                    " private-token ")
                ::get);
    var scope = mock(Config.Scope.class);
    when(scope.get(anyString(), nullable(String.class)))
        .thenAnswer(
            call ->
                Map.of("stream", "PROVIDER")
                    .getOrDefault(call.getArgument(0), call.getArgument(1)));
    try (var environment = mockStatic(Environment.class, CALLS_REAL_METHODS)) {
      environment.when(Environment::system).thenReturn(source);
      var config = BridgeConfig.from(scope);
      assertEquals("PROVIDER", config.stream());
      assertEquals(Duration.ofMillis(250), config.timeout());
      assertEquals(" private-token ", config.token());
      assertEquals("keycloak.events", config.subjectPrefix());
    }
  }

  @Test
  void blankOptionalSettingsUseDefaultsWhileExplicitSettingsAreTrimmed() {
    var config =
        BridgeConfig.from(
            Map.of(
                "stream",
                " \t",
                "subject-prefix",
                " auth.events ",
                "timeout-ms",
                " 25 ",
                "filter-file",
                " "));
    assertEquals("KEYCLOAK_EVENTS", config.stream());
    assertEquals("auth.events", config.subjectPrefix());
    assertEquals(Duration.ofMillis(25), config.timeout());
    assertNull(config.filterFile());
  }

  @Test
  void directConstructionAlsoCopiesInputServers() {
    String[] servers = {"nats://first:4222", "nats://second:4222"};
    var config = direct(Map.of("servers", servers));
    servers[0] = "nats://changed:4222";
    assertArrayEquals(new String[] {"nats://first:4222", "nats://second:4222"}, config.servers());
  }

  @ParameterizedTest
  @MethodSource("invalidDirectSettings")
  void directConstructionEnforcesRequiredFieldsAndTlsConsistency(
      String key, Object value, String reason) {
    var failure =
        assertThrows(
            IllegalArgumentException.class, () -> direct(Collections.singletonMap(key, value)));
    assertEquals(reason, failure.getMessage());
    assertNull(failure.getCause());
  }

  static Stream<Arguments> invalidDirectSettings() {
    var invalid = Stream.<Arguments>builder();
    invalid.add(Arguments.of("stream", null, "Invalid stream name"));
    invalid.add(Arguments.of("subjectPrefix", null, "Invalid subject prefix"));
    invalid.add(Arguments.of("subjectPrefix", "a".repeat(129), "Invalid subject prefix"));
    for (String name :
        new String[] {"timeout", "pollInterval", "idlePollMax", "retryInitial", "retryMax"}) {
      for (Duration value : new Duration[] {null, Duration.ZERO, Duration.ofMillis(3600001)}) {
        invalid.add(Arguments.of(name, value, "Durations must be 1..3600000 milliseconds"));
      }
    }
    invalid.add(Arguments.of("filterReloadInterval", null, "filter-reload-ms must be 100..60000"));
    invalid.add(Arguments.of("tls", null, "TLS settings must match every NATS server"));
    invalid.add(
        Arguments.of(
            "tls",
            new TlsConfig(true, null, null, null),
            "TLS settings must match every NATS server"));
    invalid.add(
        Arguments.of(
            "servers",
            new String[] {"tls://first:4222"},
            "TLS settings must match every NATS server"));
    invalid.add(
        Arguments.of(
            "servers",
            new String[] {"tls://first:4222", "nats://second:4222"},
            "TLS settings must match every NATS server"));
    return invalid.build();
  }

  private static BridgeConfig direct(Map<String, Object> values) {
    var base = BridgeConfig.from(Map.of());
    return new BridgeConfig(
        (String[]) values.getOrDefault("servers", base.servers()),
        (String) values.getOrDefault("stream", base.stream()),
        (String) values.getOrDefault("subjectPrefix", base.subjectPrefix()),
        base.minReplicas(),
        (Duration) values.getOrDefault("timeout", base.timeout()),
        (Duration) values.getOrDefault("pollInterval", base.pollInterval()),
        (Duration) values.getOrDefault("idlePollMax", base.idlePollMax()),
        base.batchSize(),
        base.relayWorkers(),
        (Duration) values.getOrDefault("retryInitial", base.retryInitial()),
        (Duration) values.getOrDefault("retryMax", base.retryMax()),
        base.maxPayloadBytes(),
        base.credentialsFile(),
        base.token(),
        (TlsConfig) values.getOrDefault("tls", base.tls()),
        base.filterFile(),
        (Duration) values.getOrDefault("filterReloadInterval", base.filterReloadInterval()),
        base.auditCleanup());
  }

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
    assertEquals(
        "BridgeConfig[stream=KEYCLOAK_EVENTS, subjectPrefix=keycloak.events]", config.toString());
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
