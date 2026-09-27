package io.github.gbeaule.keycloaknats.config;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NatsServersTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "nats://localhost",
        "tls://broker:65535",
        "nats://127.0.0.1:4222",
        "tls://[::1]:4222"
      })
  void acceptsDnsIpv4AndIpv6WithOptionalPorts(String address) {
    assertArrayEquals(new String[] {address}, NatsServers.validate(new String[] {address}));
  }

  @Test
  void validationDoesNotMutateOrShareTheCallersArray() {
    String[] input = {" nats://broker:4222 "};
    String[] validated = NatsServers.validate(input);
    assertEquals(" nats://broker:4222 ", input[0]);
    input[0] = "nats://changed";
    assertEquals("nats://broker:4222", validated[0]);
  }

  @Test
  void requiresNonNullServersAndAtLeastOneAddress() {
    assertThrows(IllegalArgumentException.class, () -> NatsServers.validate(null));
    assertThrows(IllegalArgumentException.class, () -> NatsServers.validate(new String[0]));
    assertThrows(IllegalArgumentException.class, () -> NatsServers.validate(new String[] {null}));
  }
}
