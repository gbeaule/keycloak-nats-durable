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

  Description describe(Event event) {
    Map<String, Object> data = common("user", event.getId(), event.getRealmId(), event.getError());
    data.put("userId", AffectedUser.resolve(event));
    data.put("clientId", event.getClientId());
    data.put("sessionId", event.getSessionId());
    String type = event.getType().name();
    data.put("eventType", type);
    String suffix = "user." + type.toLowerCase(Locale.ROOT);
    return description(event.getRealmId(), suffix, suffix, event.getTime(), data);
  }

  Description describe(AdminEvent event, String userId, Boolean userEnabled) {
    Map<String, Object> data = common("admin", event.getId(), event.getRealmId(), event.getError());
    data.put("resourceType", event.getResourceTypeAsString());
    data.put("operationType", event.getOperationType().name());
    data.put("resourcePath", event.getResourcePath());
    data.put("userId", userId);
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
    return description(event.getRealmId(), typeSuffix, subjectSuffix, event.getTime(), data);
  }

  record Description(
      String realmId,
      String userId,
      String type,
      String subject,
      long time,
      Map<String, Object> data) {
    Description {
      data = Map.copyOf(data);
    }
  }

  private static void putOrdering(Map<String, Object> data, EventOrdering ordering) {
    if (ordering == null) {
      return;
    }
    if (!ordering.realmId().equals(data.get("realmId"))
        || !ordering.userId().equals(data.get("userId"))) {
      throw new IllegalArgumentException("Ordering must identify the event's affected user");
    }
    data.put("ordering", ordering.wireValue());
  }

  private Description description(
      String realm, String typeSuffix, String subjectSuffix, long time, Map<String, Object> data) {
    String subject = subject(realm, subjectSuffix);
    // Storage constraints apply only after the filter selects an event for capture.
    if (subject.length() > 512) {
      throw new IllegalArgumentException("Event subject exceeds storage limit");
    }
    // Optional fields are omitted from the wire format, rather than encoded as JSON null.
    data.values().removeIf(Objects::isNull);
    return new Description(
        realm, (String) data.get("userId"), "io.keycloak." + typeSuffix, subject, time, data);
  }

  OutboxEvent serialize(
      String id,
      Description description,
      EventOrdering ordering,
      long capturedAt,
      ResolvedPublicationPolicy policy) {
    Map<String, Object> data = new LinkedHashMap<>(description.data());
    putOrdering(data, ordering);
    Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("specversion", "1.0");
    envelope.put("id", id);
    envelope.put("source", "urn:keycloak:realm:" + realmToken(description.realmId()));
    envelope.put("type", description.type());
    envelope.put("time", Instant.ofEpochMilli(description.time()).toString());
    envelope.put("datacontenttype", "application/json");
    envelope.put("dataschema", "urn:keycloak-nats:event:v1");
    envelope.put("data", data);
    try {
      String payload = writer.writeValueAsString(envelope);
      if (payload.getBytes(StandardCharsets.UTF_8).length > config.maxPayloadBytes()) {
        throw new IllegalArgumentException("Event exceeds max-payload-bytes");
      }
      return new OutboxEvent(
          id,
          description.subject(),
          payload,
          capturedAt,
          description.realmId(),
          description.type(),
          ordering,
          policy);
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
    return config.subjectPrefix() + "." + realmToken(realm) + "." + suffix;
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
