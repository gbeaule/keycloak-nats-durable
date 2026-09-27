package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

class EventCatalogueTest {
  private final EventEnvelope envelopes = new EventEnvelope(BridgeConfig.from(Map.of()));
  private final ObjectMapper objectMapper = new ObjectMapper();

  @ParameterizedTest
  @EnumSource(EventType.class)
  void everyKeycloakUserEventUsesTheDocumentedShape(EventType type) throws Exception {
    var source = EventEnvelopeTest.login();
    source.setRealmId("demo");
    source.setType(type);
    if (type.name().endsWith("_ERROR")) {
      source.setError("example_error");
    }
    var message = CaptureFixtures.user(envelopes, source);
    var event = objectMapper.readTree(message.payload());
    EventSchemaTest.assertValid(event);
    assertEquals(
        "keycloak.events.ZGVtbw.user." + type.name().toLowerCase(Locale.ROOT), message.subject());
    assertEquals(type.name(), event.at("/data/eventType").asText());
    assertEquals("user", event.at("/data/kind").asText());
    assertFalse(event.get("data").has("operationType"));
  }

  @ParameterizedTest
  @EnumSource(ResourceType.class)
  void everyAdminResourceAndOperationUsesTheDocumentedShape(ResourceType resource)
      throws Exception {
    for (OperationType operation : OperationType.values()) {
      var source = EventEnvelopeTest.admin(operation);
      source.setRealmId("demo");
      source.setResourceType(resource);
      var message = CaptureFixtures.admin(envelopes, source, null);
      var event = objectMapper.readTree(message.payload());
      EventSchemaTest.assertValid(event);
      assertEquals(
          "keycloak.events.ZGVtbw.admin."
              + resource.name().toLowerCase(Locale.ROOT)
              + "."
              + operation.name().toLowerCase(Locale.ROOT),
          message.subject());
      assertEquals(resource.name(), event.at("/data/resourceType").asText());
      assertEquals(operation.name(), event.at("/data/operationType").asText());
      assertFalse(event.get("data").has("eventType"));
    }
  }

  @Test
  void customResourcesCannotInjectSubjectTokens() {
    assertEquals("custom-", EventEnvelope.resourceToken(""));
    assertTrue(EventEnvelope.resourceToken("*.> secret").matches("custom-[A-Za-z0-9_-]+"));
    assertEquals("unknown", EventEnvelope.resourceToken(null));
    assertNotEquals(EventEnvelope.resourceToken("USER"), EventEnvelope.resourceToken("user"));
  }
}
