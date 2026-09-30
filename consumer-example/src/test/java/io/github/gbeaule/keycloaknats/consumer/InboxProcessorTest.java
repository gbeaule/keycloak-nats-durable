package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

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

  @ParameterizedTest
  @ValueSource(strings = {"specversion", "dataschema", "source", "type", "data"})
  void missingOrWronglyTypedEnvelopeFieldsCannotReachTheDatabase(String field) throws Exception {
    var mapper = new ObjectMapper();
    for (String replacement : new String[] {"null", "123", "[]", "\"unsupported\""}) {
      var event = (ObjectNode) mapper.readTree(EVENT);
      event.set(field, mapper.readTree(replacement));
      assertRejected(event.toString(), RejectedEventException.Reason.UNSUPPORTED_ENVELOPE);
    }
    var event = (ObjectNode) mapper.readTree(EVENT);
    event.remove(field);
    assertRejected(event.toString(), RejectedEventException.Reason.UNSUPPORTED_ENVELOPE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "null", "[]", "42", "true", "\"event\""})
  void onlyObjectEnvelopesAreSupported(String payload) {
    assertRejected(payload, RejectedEventException.Reason.UNSUPPORTED_ENVELOPE);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(
      strings = {
        "not-a-uuid",
        "1-1-1-1-1",
        "598DF965-0AB9-468D-9082-3FAE91FBAD70",
        " 598df965-0ab9-468d-9082-3fae91fbad70",
        "598df965-0ab9-468d-9082-3fae91fbad70 "
      })
  void eventIdsMustUseCanonicalUuidText(String id) throws Exception {
    var event = (ObjectNode) new ObjectMapper().readTree(EVENT);
    event.put("id", id);
    assertRejected(event.toString(), RejectedEventException.Reason.INVALID_EVENT_ID);
  }

  @Test
  void absentOrNumericEventIdsCannotReachTheDatabase() throws Exception {
    var event = (ObjectNode) new ObjectMapper().readTree(EVENT);
    event.remove("id");
    assertRejected(event.toString(), RejectedEventException.Reason.INVALID_EVENT_ID);
    event.put("id", 123);
    assertRejected(event.toString(), RejectedEventException.Reason.INVALID_EVENT_ID);
  }

  @Test
  void payloadSizeLimitIsInclusiveAndCheckedBeforeParsingOrDatabaseAcquisition() {
    var processor = new InboxProcessor(database(), "worker");
    for (byte[] payload : new byte[][] {null, new byte[1048577]}) {
      var failure =
          assertThrows(
              RejectedEventException.class,
              () -> processor.process(payload, (db, event) -> fail()));
      assertEquals(RejectedEventException.Reason.OVERSIZE, failure.reason());
    }
    byte[] maximum =
        (EVENT + " ".repeat(1048576 - EVENT.getBytes(StandardCharsets.UTF_8).length))
            .getBytes(StandardCharsets.UTF_8);
    var reachedDatabase =
        assertThrows(SQLException.class, () -> processor.process(maximum, (db, event) -> fail()));
    assertEquals("08006", reachedDatabase.getSQLState());
  }

  private static void assertRejected(String payload, RejectedEventException.Reason reason) {
    var processor = new InboxProcessor(database(), "worker");
    var failure =
        assertThrows(
            RejectedEventException.class,
            () ->
                processor.process(payload.getBytes(StandardCharsets.UTF_8), (db, event) -> fail()));
    assertEquals(reason, failure.reason(), payload);
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
