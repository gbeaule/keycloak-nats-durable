package io.github.gbeaule.keycloaknats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.keycloak.events.Event;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.ResourceType;

/**
 * An explicit allowlist avoids sending representations, passwords, tokens or arbitrary event
 * details.
 */
public final class EventEnvelope {
  private static final ObjectWriter defaultWriter = new ObjectMapper().writer();
  private final BridgeConfig config;
  private final ObjectWriter writer;

  /** Uses the configured routing prefix and serialized payload limit. */
  public EventEnvelope(BridgeConfig config) {
    this(config, defaultWriter);
  }

  EventEnvelope(BridgeConfig config, ObjectWriter writer) {
    this.config = config;
    this.writer = writer;
  }

  /** Returns the routing subject without serializing or allocating an event identity. */
  public String userSubject(Event event) {
    return subject(event.getRealmId(), "user." + event.getType().name().toLowerCase(Locale.ROOT));
  }

  /** Returns the admin routing subject before looking up observed user state. */
  public String adminSubject(AdminEvent event) {
    return subject(
        event.getRealmId(),
        "admin."
            + resourceToken(event.getResourceTypeAsString())
            + "."
            + event.getOperationType().name().toLowerCase(Locale.ROOT));
  }

  /** Creates a user event with a new identity that is persisted for all relay retries. */
  public OutboxEvent user(Event event) {
    Map<String, Object> data = common("user", event.getId(), event.getRealmId(), event.getError());
    data.put("userId", event.getUserId());
    data.put("clientId", event.getClientId());
    String type = event.getType().name();
    data.put("eventType", type);
    String suffix = "user." + type.toLowerCase(Locale.ROOT);
    return envelope(event.getRealmId(), suffix, suffix, event.getTime(), data);
  }

  /** Creates an admin event; enablement is a nullable state observation, not a transition. */
  public OutboxEvent admin(AdminEvent event, Boolean userEnabled) {
    Map<String, Object> data = common("admin", event.getId(), event.getRealmId(), event.getError());
    data.put("resourceType", event.getResourceTypeAsString());
    data.put("operationType", event.getOperationType().name());
    data.put("resourcePath", event.getResourcePath());
    data.put("userId", targetUserId(event));
    data.put("userEnabled", userEnabled);
    if (event.getAuthDetails() != null) {
      data.put("actorUserId", event.getAuthDetails().getUserId());
      data.put("actorRealmId", event.getAuthDetails().getRealmId());
      data.put("clientId", event.getAuthDetails().getClientId());
    }
    String operation = event.getOperationType().name().toLowerCase(Locale.ROOT);
    String typeSuffix = "admin." + operation;
    String subjectSuffix =
        "admin." + resourceToken(event.getResourceTypeAsString()) + "." + operation;
    return envelope(event.getRealmId(), typeSuffix, subjectSuffix, event.getTime(), data);
  }

  /** Returns the target of a direct user operation, or null for nested and other resources. */
  public static String targetUserId(AdminEvent event) {
    if (!"USER".equals(event.getResourceTypeAsString())) {
      return null;
    }
    String path = event.getResourcePath();
    if (path == null || !path.startsWith("users/")) {
      return null;
    }
    String id = path.substring(6);
    return id.isEmpty() || id.contains("/") ? null : id;
  }

  private OutboxEvent envelope(
      String realm, String typeSuffix, String subjectSuffix, long time, Map<String, Object> data) {
    // Optional fields are omitted from the wire format, rather than encoded as JSON null.
    data.values().removeIf(Objects::isNull);
    String realmToken = realmToken(realm);
    final String subject = subject(realm, subjectSuffix);
    String id = UUID.randomUUID().toString();
    Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("specversion", "1.0");
    envelope.put("id", id);
    envelope.put("source", "urn:keycloak:realm:" + realmToken);
    envelope.put("type", "io.keycloak." + typeSuffix);
    envelope.put("time", Instant.ofEpochMilli(time).toString());
    envelope.put("datacontenttype", "application/json");
    envelope.put("dataschema", "urn:keycloak-nats:event:v1");
    envelope.put("data", data);
    try {
      String payload = writer.writeValueAsString(envelope);
      if (payload.getBytes(StandardCharsets.UTF_8).length > config.maxPayloadBytes()) {
        throw new IllegalArgumentException("Event exceeds max-payload-bytes");
      }
      return new OutboxEvent(id, subject, payload, System.currentTimeMillis());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize event");
    }
  }

  static String resourceToken(String resourceType) {
    if (resourceType == null) {
      return "unknown";
    }
    try {
      return ResourceType.valueOf(resourceType).name().toLowerCase(Locale.ROOT);
    } catch (IllegalArgumentException customResource) {
      // Custom resource names are data, never wildcards or multiple subject tokens.
      return "custom-"
          + Base64.getUrlEncoder()
              .withoutPadding()
              .encodeToString(resourceType.getBytes(StandardCharsets.UTF_8));
    }
  }

  private String subject(String realm, String suffix) {
    String result = config.subjectPrefix() + "." + realmToken(realm) + "." + suffix;
    if (result.length() > 512) {
      throw new IllegalArgumentException("Event subject exceeds storage limit");
    }
    return result;
  }

  private static String realmToken(String realm) {
    if (realm == null || realm.isBlank()) {
      throw new IllegalArgumentException("Event realm is required");
    }
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(realm.getBytes(StandardCharsets.UTF_8));
  }

  private static Map<String, Object> common(String kind, String id, String realm, String error) {
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("kind", kind);
    data.put("keycloakEventId", id);
    data.put("realmId", realm);
    data.put("outcome", error == null ? "success" : "error");
    data.put("error", error);
    return data;
  }
}
