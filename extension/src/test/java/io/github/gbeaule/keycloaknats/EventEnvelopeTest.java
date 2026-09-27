package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.BooleanNode;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

class EventEnvelopeTest {
  private final EventEnvelope encoder = new EventEnvelope(BridgeConfig.from(Map.of()));

  static Event login() {
    Event event = new Event();
    event.setType(EventType.LOGIN);
    event.setRealmId("tenant.* >.é");
    event.setTime(1000);
    event.setId("source-event");
    event.setUserId("target-user");
    event.setDetails(
        Map.of(
            "password",
            "sensitive-password",
            "token",
            "sensitive-token",
            "username",
            "sensitive-username"));
    event.setIpAddress("192.0.2.1");
    return event;
  }

  static AdminEvent admin(OperationType operation) {
    AdminEvent event = new AdminEvent();
    event.setId("admin-source");
    event.setRealmId("realm");
    event.setResourceType(ResourceType.USER);
    event.setOperationType(operation);
    event.setResourcePath("users/target");
    event.setRepresentation(
        "{\"password\":\"sensitive-password\",\"email\":\"private@example.com\"}");
    return event;
  }

  @Test
  void createsValidEnvelopeWithSafeDistinctRealmTokens() throws Exception {
    final long before = System.currentTimeMillis();
    var event = encoder.user(login());
    final long after = System.currentTimeMillis();
    var json = new ObjectMapper().readTree(event.payload());
    Set<String> fields = new HashSet<>();
    json.fieldNames().forEachRemaining(fields::add);
    assertEquals(
        Set.of(
            "specversion", "id", "time", "source", "type", "datacontenttype", "dataschema", "data"),
        fields);
    assertEquals("1.0", json.get("specversion").asText());
    assertEquals(event.id(), json.get("id").asText());
    assertEquals("1970-01-01T00:00:01Z", json.get("time").asText());
    assertEquals("source-event", json.at("/data/keycloakEventId").asText());
    assertEquals("io.keycloak.user.login", json.get("type").textValue());
    assertEquals("application/json", json.get("datacontenttype").textValue());
    assertEquals("urn:keycloak-nats:event:v1", json.get("dataschema").textValue());
    String realmToken = event.subject().split("\\.")[2];
    assertEquals("urn:keycloak:realm:" + realmToken, json.get("source").textValue());
    assertEquals(
        login().getRealmId(),
        new String(Base64.getUrlDecoder().decode(realmToken), StandardCharsets.UTF_8));
    assertEquals(4, UUID.fromString(event.id()).version());
    assertTrue(event.createdAt() >= before && event.createdAt() <= after);
    assertEquals(event.createdAt(), event.nextAttemptAt());
    assertEquals(0, event.attempts());
    assertNull(event.lastError());
    assertEquals(
        new ObjectMapper()
            .valueToTree(
                Map.of(
                    "kind",
                    "user",
                    "keycloakEventId",
                    "source-event",
                    "realmId",
                    login().getRealmId(),
                    "outcome",
                    "success",
                    "userId",
                    "target-user",
                    "eventType",
                    "LOGIN")),
        json.get("data"));
    assertTrue(event.subject().matches("keycloak\\.events\\.[A-Za-z0-9_-]+\\.user\\.login"));
    var other = login();
    other.setRealmId("tenant");
    assertNotEquals(event.subject(), encoder.user(other).subject());
  }

  @Test
  void privateDetailsNeverLeaveKeycloakAndNullClientIsSupported() {
    String payload = encoder.user(login()).payload();
    assertFalse(payload.contains("sensitive-"));
    assertFalse(payload.contains("192.0.2.1"));
    assertFalse(payload.contains("clientId"));
  }

  @Test
  void disabledUserStateSurvivesWithoutAdminRepresentation() throws Exception {
    var event = encoder.admin(admin(OperationType.UPDATE), false);
    var json = new ObjectMapper().readTree(event.payload());
    assertEquals(BooleanNode.FALSE, json.at("/data/userEnabled"));
    assertEquals("target", json.at("/data/userId").asText());
    assertEquals("UPDATE", json.at("/data/operationType").asText());
    assertFalse(event.payload().contains("sensitive-"));
    assertFalse(event.payload().contains("private@example"));
  }

  @Test
  void deleteDoesNotRequireLookingUpTheDeletedUser() throws Exception {
    var json =
        new ObjectMapper().readTree(encoder.admin(admin(OperationType.DELETE), null).payload());
    assertEquals("target", json.at("/data/userId").textValue());
    assertEquals("DELETE", json.at("/data/operationType").textValue());
    assertEquals("io.keycloak.admin.delete", json.get("type").textValue());
    assertFalse(json.get("data").has("userEnabled"));
  }

  @ParameterizedTest
  @CsvSource({"users/a,a", "users/a/roles,", "users/,", "clients/a,", "users/a/b,", ","})
  void onlyDirectUserResourcesAreTreatedAsAccountChanges(String path, String expected) {
    var event = admin(OperationType.UPDATE);
    event.setResourcePath(path);
    assertEquals(expected, EventEnvelope.targetUserId(event));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"CLIENT", "user", "widget"})
  void otherResourcesNeverInventTargetUserIdentityEvenWithUserPaths(String resource)
      throws Exception {
    var event = admin(OperationType.UPDATE);
    event.setResourceTypeAsString(resource);
    assertNull(EventEnvelope.targetUserId(event));
    var json = new ObjectMapper().readTree(encoder.admin(event, null).payload());
    assertFalse(json.get("data").has("userId"));
  }

  @Test
  void failureEventsHaveAnErrorOutcome() throws Exception {
    var event = login();
    event.setType(EventType.LOGIN_ERROR);
    event.setError("invalid_user_credentials");
    var row = encoder.user(event);
    var json = new ObjectMapper().readTree(row.payload());
    assertEquals("error", json.at("/data/outcome").textValue());
    assertEquals("invalid_user_credentials", json.at("/data/error").textValue());
    assertEquals("LOGIN_ERROR", json.at("/data/eventType").textValue());
    assertEquals("io.keycloak.user.login_error", json.get("type").textValue());
    assertTrue(row.subject().endsWith(".user.login_error"));
  }

  @Test
  void adminActorAndTargetRemainDistinctAndOnlyAllowlistedFieldsAreSerialized() throws Exception {
    final var event = admin(OperationType.UPDATE);
    var actor = new AuthDetails();
    actor.setUserId("actor");
    actor.setRealmId("management-realm");
    actor.setClientId("admin-console");
    actor.setIpAddress("192.0.2.55");
    event.setAuthDetails(actor);
    var row = encoder.admin(event, true);
    var json = new ObjectMapper().readTree(row.payload());
    assertEquals(
        new ObjectMapper()
            .readTree(
                """
                {"kind":"admin","keycloakEventId":"admin-source","realmId":"realm",
                 "outcome":"success","resourceType":"USER","operationType":"UPDATE",
                 "resourcePath":"users/target","userId":"target","userEnabled":true,
                 "actorUserId":"actor","actorRealmId":"management-realm","clientId":"admin-console"}
                """),
        json.get("data"));
    assertEquals("keycloak.events.cmVhbG0.admin.user.update", row.subject());
    assertEquals("io.keycloak.admin.update", json.get("type").textValue());
    assertFalse(row.payload().contains("sensitive-password"));
    assertFalse(row.payload().contains("192.0.2.55"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t\n"})
  void absentRealmsFailWithSafeErrorsBeforeCapture(String realm) {
    var event = login();
    event.setRealmId(realm);
    var failure = assertThrows(IllegalArgumentException.class, () -> encoder.user(event));
    assertEquals("Event realm is required", failure.getMessage());
    assertNull(failure.getCause());
  }

  @Test
  void subjectStorageLimitIsInclusive() {
    var event = admin(OperationType.UPDATE);
    event.setRealmId("r".repeat(358));
    assertEquals(512, encoder.adminSubject(event).length());
    assertEquals(512, encoder.admin(event, false).subject().length());
    event.setRealmId("r".repeat(359));
    var failure = assertThrows(IllegalArgumentException.class, () -> encoder.admin(event, false));
    assertEquals("Event subject exceeds storage limit", failure.getMessage());
  }

  @Test
  void payloadLimitCountsUtf8BytesAndIncludesTheExactBoundary() {
    var event = login();
    event.setUserId("é".repeat(255));
    event.setClientId("é".repeat(100));
    String payload = encoder.user(event).payload();
    int bytes = payload.getBytes(StandardCharsets.UTF_8).length;
    var exact =
        new EventEnvelope(BridgeConfig.from(Map.of("max-payload-bytes", Integer.toString(bytes))));
    assertEquals(bytes, exact.user(event).payload().getBytes(StandardCharsets.UTF_8).length);
    event.setClientId(event.getClientId() + "x");
    var failure = assertThrows(IllegalArgumentException.class, () -> exact.user(event));
    assertEquals("Event exceeds max-payload-bytes", failure.getMessage());
    assertNull(failure.getCause());
  }

  @Test
  void serializationFailureRejectsTheEventWithoutExposingItsDataOrCause() throws Exception {
    var writer = mock(ObjectWriter.class);
    when(writer.writeValueAsString(any()))
        .thenThrow(new JsonMappingException(null, "private-event-data"));
    var constrained = new EventEnvelope(BridgeConfig.from(Map.of()), writer);
    var failure = assertThrows(IllegalStateException.class, () -> constrained.user(login()));
    assertEquals("Cannot serialize event", failure.getMessage());
    assertNull(failure.getCause());
  }

  @Test
  void oversizeOrMissingRealmFailsRatherThanDroppingFields() {
    var event = login();
    event.setUserId("é".repeat(255));
    event.setClientId("é".repeat(255));
    var small = new EventEnvelope(BridgeConfig.from(Map.of("max-payload-bytes", "1024")));
    assertThrows(IllegalArgumentException.class, () -> small.user(event));
    event.setRealmId(null);
    assertThrows(IllegalArgumentException.class, () -> encoder.user(event));
  }

  @Test
  void eachCaptureHasItsOwnTransportIdentity() {
    assertNotEquals(encoder.user(login()).id(), encoder.user(login()).id());
  }
}
