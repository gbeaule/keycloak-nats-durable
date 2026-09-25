package io.github.keycloaknats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.keycloak.events.Event;
import org.keycloak.events.admin.AdminEvent;

/**
 * An explicit allowlist avoids sending representations, passwords, tokens or arbitrary event
 * details.
 */
public final class EventEnvelope {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final BridgeConfig config;

  public EventEnvelope(BridgeConfig config) {
    this.config = config;
  }

  public OutboxEvent user(Event event) {
    Map<String, Object> data = common("user", event.getId(), event.getRealmId(), event.getError());
    put(data, "userId", event.getUserId());
    put(data, "clientId", event.getClientId());
    String type = event.getType().name();
    data.put("eventType", type);
    return envelope(
        event.getRealmId(), "user." + type.toLowerCase(Locale.ROOT), event.getTime(), data);
  }

  public OutboxEvent admin(AdminEvent event, Boolean userEnabled) {
    Map<String, Object> data = common("admin", event.getId(), event.getRealmId(), event.getError());
    put(data, "resourceType", event.getResourceTypeAsString());
    put(data, "operationType", event.getOperationType().name());
    put(data, "resourcePath", event.getResourcePath());
    put(data, "userId", targetUserId(event));
    put(data, "userEnabled", userEnabled);
    if (event.getAuthDetails() != null) {
      put(data, "actorUserId", event.getAuthDetails().getUserId());
      put(data, "actorRealmId", event.getAuthDetails().getRealmId());
      put(data, "clientId", event.getAuthDetails().getClientId());
    }
    // Resource names may be supplied by other extensions. Only the fixed operation is a subject
    // token.
    return envelope(
        event.getRealmId(),
        "admin." + event.getOperationType().name().toLowerCase(Locale.ROOT),
        event.getTime(),
        data);
  }

  public static String targetUserId(AdminEvent event) {
    if (!"USER".equals(event.getResourceTypeAsString())) return null;
    String path = event.getResourcePath();
    if (path == null || !path.startsWith("users/")) return null;
    String id = path.substring(6);
    return id.isEmpty() || id.contains("/") ? null : id;
  }

  private OutboxEvent envelope(String realm, String suffix, long time, Map<String, Object> data) {
    if (realm == null || realm.isBlank())
      throw new IllegalArgumentException("Event realm is required");
    String realmToken =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(realm.getBytes(StandardCharsets.UTF_8));
    String subject = config.subjectPrefix() + "." + realmToken + "." + suffix;
    if (subject.length() > 512)
      throw new IllegalArgumentException("Event subject exceeds storage limit");
    String id = UUID.randomUUID().toString();
    Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("specversion", "1.0");
    envelope.put("id", id);
    envelope.put("source", "urn:keycloak:realm:" + realmToken);
    envelope.put("type", "io.keycloak." + suffix);
    envelope.put("time", Instant.ofEpochMilli(time).toString());
    envelope.put("datacontenttype", "application/json");
    envelope.put("dataschema", "urn:keycloak-nats:event:v1");
    envelope.put("data", data);
    try {
      String payload = JSON.writeValueAsString(envelope);
      if (payload.getBytes(StandardCharsets.UTF_8).length > config.maxPayloadBytes())
        throw new IllegalArgumentException("Event exceeds max-payload-bytes");
      return new OutboxEvent(id, subject, payload, System.currentTimeMillis());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize event");
    }
  }

  private static Map<String, Object> common(String kind, String id, String realm, String error) {
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("kind", kind);
    put(data, "keycloakEventId", id);
    put(data, "realmId", realm);
    data.put("outcome", error == null ? "success" : "error");
    put(data, "error", error);
    return data;
  }

  private static void put(Map<String, Object> into, String key, Object value) {
    if (value != null) into.put(key, value);
  }
}
