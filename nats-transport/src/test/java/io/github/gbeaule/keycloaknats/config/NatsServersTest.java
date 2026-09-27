package io.github.gbeaule.keycloaknats.config;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NatsServersTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "nats://localhost",
        "nats://broker:1",
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " ",
        "\t",
        "nats://user:secret@broker:4222",
        "nats://secret@broker",
        "tls://user:secret@broker",
        "nats://user:secret@bad host",
        "http://broker:4222",
        "broker:4222",
        "nats:///",
        "nats://broker/path",
        "nats://broker/",
        "nats://broker?secret",
        "nats://broker#secret",
        "nats://broker:0",
        "nats://broker:65536",
        "nats://broker:-1"
      })
  void rejectsUnsafeOrMalformedSeedsWithoutExposingTheInput(String address) {
    var failure =
        assertThrows(
            IllegalArgumentException.class, () -> NatsServers.validate(new String[] {address}));
    String expected =
        address.isBlank()
            ? "NATS server entries must not be empty"
            : address.contains("bad host")
                ? "Invalid NATS server URI"
                : "Use nats://host:port or tls://host:port; use separate credentials";
    assertEquals(expected, failure.getMessage());
    assertNull(
        failure.getCause(), "URI parsing must not leak credentials through an exception cause");
  }

  @Test
  void preservesSeedOrderAndLeavesTheInputUntouchedEvenWhenValidationFails() {
    String[] input = {" nats://first:4222 ", "tls://second:4222"};
    assertArrayEquals(
        new String[] {"nats://first:4222", "tls://second:4222"}, NatsServers.validate(input));
    input[1] = "nats://user:secret@second";
    assertThrows(IllegalArgumentException.class, () -> NatsServers.validate(input));
    assertArrayEquals(new String[] {" nats://first:4222 ", "nats://user:secret@second"}, input);
  }
}
