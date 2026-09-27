package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;

class EventOrderingTest {
  private final EventEnvelope envelopes = new EventEnvelope(BridgeConfig.from(Map.of()));
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void keysEncodeTheExactRealmAndUserTupleIncludingStorageBackedIds() {
    String realm = "tenant.é";
    String user = "f:provider:user.* >.用户";
    var ordering = new EventOrdering(realm, user, Long.MAX_VALUE);
    String[] parts = ordering.key().split("\\.");
    assertEquals(3, parts.length);
    assertEquals("u", parts[0]);
    assertEquals(
        realm, new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8));
    assertEquals(user, new String(Base64.getUrlDecoder().decode(parts[2]), StandardCharsets.UTF_8));
    assertNotEquals(new EventOrdering("a.b", "c", 1).key(), new EventOrdering("a", "b.c", 1).key());
    assertNotEquals(ordering.key(), new EventOrdering("other", user, 1).key());
    assertEquals("9223372036854775807", ordering.wireValue().get("sequence"));
    assertEquals(Long.MAX_VALUE, EventOrdering.parseSequence("9223372036854775807"));
    assertEquals(1, EventOrdering.parseSequence("1"));
  }

  @Test
  void orderingValuesRemainInterchangeableAsMapKeys() {
    var ordering = new EventOrdering("realm", "user", 1);
    var positions = new HashMap<>(Map.of(ordering, "captured"));
    assertEquals("captured", positions.get(new EventOrdering("realm", "user", 1)));
    assertNotEquals(ordering, new EventOrdering("realm", "user", 2));
    assertNotEquals(ordering, new EventOrdering("other-realm", "user", 1));
    assertNotEquals(ordering, new EventOrdering("realm", "other-user", 1));
    assertNotEquals(ordering, "not an ordering");
    assertNotEquals(ordering, null);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(
      strings = {
        "0",
        "-1",
        "+1",
        "01",
        "1.0",
        "1e3",
        " 1",
        "1\n",
        "١",
        "9223372036854775808",
        "99999999999999999999999999999999"
      })
  void malformedOrOverflowingSequencesAreRejected(String sequence) {
    assertThrows(IllegalArgumentException.class, () -> EventOrdering.parseSequence(sequence));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t", "\uD800"})
  void identitiesMustBeNonblankAndLosslesslyEncoded(String identity) {
    assertThrows(IllegalArgumentException.class, () -> new EventOrdering(identity, "user", 1));
    assertThrows(IllegalArgumentException.class, () -> new EventOrdering("realm", identity, 1));
  }

  @ParameterizedTest
  @ValueSource(longs = {0, -1, Long.MIN_VALUE})
  void storageSequencesArePositive(long sequence) {
    assertThrows(
        IllegalArgumentException.class, () -> new EventOrdering("realm", "user", sequence));
  }

  @Test
  void identifierAndIndexLimitsRejectOversizeValuesWithoutTruncation() {
    assertEquals("x".repeat(255), new EventOrdering("realm", "x".repeat(255), 1).userId());
    assertThrows(
        IllegalArgumentException.class, () -> new EventOrdering("realm", "x".repeat(256), 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EventOrdering("😀".repeat(255), "😀".repeat(255), 1));
  }

  @Test
  void orderingPreservesBusinessSubjectsAndCloudEventIdentity() throws Exception {
    var user = EventEnvelopeTest.login();
    var ordering = new EventOrdering(user.getRealmId(), user.getUserId(), 12);
    var row = CaptureFixtures.user(envelopes, user, ordering);
    var json = mapper.readTree(row.payload());
    assertEquals(row.id(), json.get("id").textValue());
    assertEquals(envelopes.userSubject(user), row.subject());
    assertEquals("urn:keycloak-nats:event:v1", json.get("dataschema").textValue());
    assertEquals(mapper.valueToTree(ordering.wireValue()), json.at("/data/ordering"));
    EventSchemaTest.assertValid(json);
    var admin = EventEnvelopeTest.admin(OperationType.DELETE);
    admin.setAuthDetails(new AuthDetails());
    admin.getAuthDetails().setUserId("actor");
    admin.getAuthDetails().setRealmId("master");
    ordering = new EventOrdering("realm", "target", Long.MAX_VALUE);
    row = CaptureFixtures.admin(envelopes, admin, null, ordering);
    json = mapper.readTree(row.payload());
    assertEquals(mapper.valueToTree(ordering.wireValue()), json.at("/data/ordering"));
    assertEquals(envelopes.adminSubject(admin), row.subject());
    EventSchemaTest.assertValid(json);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CaptureFixtures.admin(envelopes, admin, null, new EventOrdering("realm", "actor", 1)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CaptureFixtures.admin(
                envelopes, admin, null, new EventOrdering("master", "target", 1)));
  }

  @Test
  void userlessEventsOmitOrderingAndCannotBorrowAnActorsPosition() throws Exception {
    var user = EventEnvelopeTest.login();
    user.setUserId(null);
    assertFalse(
        mapper
            .readTree(CaptureFixtures.user(envelopes, user).payload())
            .get("data")
            .has("ordering"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CaptureFixtures.user(
                envelopes, user, new EventOrdering(user.getRealmId(), "other", 1)));
    var admin = EventEnvelopeTest.admin(OperationType.ACTION);
    admin.setResourcePath("clients/client");
    assertFalse(
        mapper
            .readTree(CaptureFixtures.admin(envelopes, admin, null).payload())
            .get("data")
            .has("ordering"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CaptureFixtures.admin(envelopes, admin, null, new EventOrdering("realm", "actor", 1)));
  }
}
