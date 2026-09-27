package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class InboxProcessorTest {
  private static final String EVENT =
      """
      {"specversion":"1.0","id":"598df965-0ab9-468d-9082-3fae91fbad70",
       "source":"urn:keycloak:realm:demo","type":"io.keycloak.user.login",
       "dataschema":"urn:keycloak-nats:event:v1","data":{"kind":"user"}}
      """;

  @ParameterizedTest
  @MethodSource("ambiguousJson")
  void invalidJsonIsRejectedBeforeDatabaseAcquisition(String payload) {
    var processor = new InboxProcessor(database(), "worker");
    var failure =
        assertThrows(
            RejectedEventException.class,
            () ->
                processor.process(payload.getBytes(StandardCharsets.UTF_8), (db, event) -> fail()));
    assertEquals(RejectedEventException.Reason.INVALID_JSON, failure.reason());
  }

  static Stream<String> ambiguousJson() {
    return Stream.of(
        EVENT + "{}",
        EVENT + "null",
        EVENT + "not-json",
        EVENT.replace("\"specversion\":", "\"specversion\":\"0.3\",\"specversion\":"),
        EVENT.replace("\"kind\":\"user\"", "\"kind\":\"admin\",\"kind\":\"user\""));
  }

  @Test
  void envelopeVersionMustBeText() {
    var processor = new InboxProcessor(database(), "worker");
    var failure =
        assertThrows(
            RejectedEventException.class,
            () ->
                processor.process(
                    EVENT.replace("\"1.0\"", "1.0").getBytes(StandardCharsets.UTF_8),
                    (db, event) -> fail()));
    assertEquals(RejectedEventException.Reason.UNSUPPORTED_ENVELOPE, failure.reason());
  }

  @Test
  void singleEnvelopeWithTrailingWhitespaceReachesTheDatabase() {
    var processor = new InboxProcessor(database(), "worker");
    var failure =
        assertThrows(
            SQLException.class,
            () ->
                processor.process(
                    (EVENT + " \r\n\t").getBytes(StandardCharsets.UTF_8), (db, event) -> fail()));
    assertEquals("08006", failure.getSQLState());
  }

  private static DataSource database() {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if ("getConnection".equals(method.getName())) {
                throw new SQLException("Database acquisition reached", "08006");
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
