package io.github.gbeaule.keycloaknats;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

/** Immutable capture policy. Excluded events never enter the durable outbox. */
final class EventFilter {
  private static final ObjectMapper objectMapper =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final Set<String> userEvents;
  private final List<AdminRule> adminEvents;
  private final CaptureScope scope;

  private record AdminRule(String resource, Set<String> operations, Boolean userEnabled) {
    boolean matches(AdminEvent event) {
      return (resource.equals("*") || resource.equals(event.getResourceTypeAsString()))
          && (operations.contains("*") || operations.contains(event.getOperationType().name()));
    }
  }

  private EventFilter(Set<String> userEvents, List<AdminRule> adminEvents, CaptureScope scope) {
    this.userEvents = Set.copyOf(userEvents);
    this.adminEvents = List.copyOf(adminEvents);
    this.scope = scope;
  }

  static EventFilter all() {
    return new EventFilter(
        Set.of("*"), List.of(new AdminRule("*", Set.of("*"), null)), CaptureScope.all());
  }

  static EventFilter parse(byte[] bytes) throws IOException {
    JsonNode root = objectMapper.readTree(bytes);
    fields(
        root, Set.of("userEvents", "adminEvents", "realmIds", "clientIds", "outcomes", "subjects"));
    Set<String> users = names(root.get("userEvents"), EventType.class);
    JsonNode admins = root.get("adminEvents");
    require(admins != null && admins.isArray(), "adminEvents must be an array");
    List<AdminRule> rules = new ArrayList<>();
    for (JsonNode rule : admins) {
      fields(rule, Set.of("resourceType", "operations", "userEnabled"));
      JsonNode resource = rule.get("resourceType");
      require(resource != null && resource.isTextual(), "resourceType is required");
      String type = resource.textValue();
      // Custom resources need an explicit prefix so a misspelled built-in type fails validation.
      if (!type.equals("*") && !type.startsWith("custom:")) {
        enumName(type, ResourceType.class);
      } else if (type.startsWith("custom:")) {
        type = type.substring(7);
        require(!type.isBlank() && !type.equals("*"), "Custom resource name is required");
      }
      Set<String> operations = names(rule.get("operations"), OperationType.class);
      Boolean enabled = null;
      if (rule.has("userEnabled")) {
        require(rule.get("userEnabled").isBoolean(), "userEnabled must be a boolean");
        require(
            type.equals("USER")
                && !operations.isEmpty()
                && Set.of("CREATE", "UPDATE").containsAll(operations),
            "userEnabled requires USER and only CREATE/UPDATE operations");
        enabled = rule.get("userEnabled").booleanValue();
      }
      rules.add(new AdminRule(type, operations, enabled));
    }
    return new EventFilter(users, rules, CaptureScope.parse(root));
  }

  boolean accepts(Event event, String subject) {
    return (userEvents.contains("*") || userEvents.contains(event.getType().name()))
        && scope.accepts(event.getRealmId(), event.getClientId(), event.getError(), subject);
  }

  boolean accepts(AdminEvent event, Boolean enabled, String subject) {
    return inScope(event, subject)
        && adminEvents.stream()
            .anyMatch(
                rule ->
                    rule.matches(event)
                        && (rule.userEnabled() == null
                            || (event.getError() == null
                                && EventEnvelope.targetUserId(event) != null
                                && rule.userEnabled().equals(enabled))));
  }

  boolean mayAccept(AdminEvent event, String subject) {
    return inScope(event, subject) && adminEvents.stream().anyMatch(rule -> rule.matches(event));
  }

  private boolean inScope(AdminEvent event, String subject) {
    String client = event.getAuthDetails() == null ? null : event.getAuthDetails().getClientId();
    return scope.accepts(event.getRealmId(), client, event.getError(), subject);
  }

  private static void fields(JsonNode node, Set<String> allowed) {
    require(node.isObject(), "Expected an object");
    node.fieldNames()
        .forEachRemaining(name -> require(allowed.contains(name), "Unknown filter field"));
  }

  private static <E extends Enum<E>> Set<String> names(JsonNode node, Class<E> type) {
    require(node != null && node.isArray(), "Event and operation lists must be arrays");
    Set<String> names = new HashSet<>();
    for (JsonNode value : node) {
      require(value.isTextual(), "Event and operation names must be strings");
      String name = value.textValue();
      if (!name.equals("*")) {
        enumName(name, type);
      }
      require(names.add(name), "Duplicate filter value");
    }
    require(!names.contains("*") || names.size() == 1, "Wildcard must be the only value");
    return Set.copyOf(names);
  }

  private static <E extends Enum<E>> void enumName(String name, Class<E> type) {
    try {
      Enum.valueOf(type, name);
    } catch (IllegalArgumentException invalid) {
      throw new IllegalArgumentException("Unknown event, operation or resource name");
    }
  }

  private static void require(boolean condition, String reason) {
    if (!condition) {
      throw new IllegalArgumentException(reason);
    }
  }
}
