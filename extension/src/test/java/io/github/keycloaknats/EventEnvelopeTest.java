package io.github.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
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
    var event = encoder.user(login());
    var json = new ObjectMapper().readTree(event.payload());
    assertEquals("1.0", json.get("specversion").asText());
    assertEquals(event.id(), json.get("id").asText());
    assertEquals("1970-01-01T00:00:01Z", json.get("time").asText());
    assertEquals("source-event", json.at("/data/keycloakEventId").asText());
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
    assertFalse(json.at("/data/userEnabled").asBoolean());
    assertEquals("target", json.at("/data/userId").asText());
    assertEquals("UPDATE", json.at("/data/operationType").asText());
    assertFalse(event.payload().contains("sensitive-"));
    assertFalse(event.payload().contains("private@example"));
  }

  @Test
  void deleteDoesNotRequireLookingUpTheDeletedUser() {
    String json = encoder.admin(admin(OperationType.DELETE), null).payload();
    assertTrue(json.contains("target"));
    assertFalse(json.contains("userEnabled"));
  }

  @ParameterizedTest
  @CsvSource({"users/a,a", "users/a/roles,", "users/,", "clients/a,", "users/a/b,"})
  void onlyDirectUserResourcesAreTreatedAsAccountChanges(String path, String expected) {
    var event = admin(OperationType.UPDATE);
    event.setResourcePath(path);
    assertEquals(expected, EventEnvelope.targetUserId(event));
  }

  @Test
  void failureEventsHaveAnErrorOutcome() {
    var event = login();
    event.setType(EventType.LOGIN_ERROR);
    event.setError("invalid_user_credentials");
    String json = encoder.user(event).payload();
    assertTrue(json.contains("\"outcome\":\"error\""));
    assertTrue(json.contains("LOGIN_ERROR"));
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
