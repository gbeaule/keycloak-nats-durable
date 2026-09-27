package io.github.gbeaule.keycloaknats;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

/** One immutable capture and publication-policy snapshot, including its exact input digest. */
final class EventFilter {
  private static final ObjectMapper objectMapper =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  // With no file configured, provenance hashes this exact equivalent filter document.
  private static final EventFilter defaultFilter =
      new EventFilter(
          Set.of("*"),
          List.of(new AdminRule("*", Set.of("*"), null)),
          CaptureScope.all(),
          List.of(),
          digest(
              ("{\"userEvents\":[\"*\"],\"adminEvents\":["
                      + "{\"resourceType\":\"*\",\"operations\":[\"*\"]}]}")
                  .getBytes(StandardCharsets.UTF_8)));
  private final Set<String> userEvents;
  private final List<AdminRule> adminEvents;
  private final CaptureScope scope;
  private final List<DeliveryRule> deliveryRules;
  private final String sha256;

  private record DeliveryRule(
      Set<String> eventTypes,
      AdminRule admin,
      CaptureScope scope,
      ResolvedPublicationPolicy resolved) {
    boolean matches(Event event, String subject) {
      return (eventTypes.contains("*") || eventTypes.contains(event.getType().name()))
          && scope.accepts(event.getRealmId(), event.getClientId(), event.getError(), subject);
    }

    boolean matches(AdminEvent event, Boolean enabled, String subject) {
      return admin != null && admin.matches(event, enabled) && inScope(scope, event, subject);
    }
  }

  private record AdminRule(String resource, Set<String> operations, Boolean userEnabled) {
    boolean matches(AdminEvent event) {
      return (resource.equals("*") || resource.equals(event.getResourceTypeAsString()))
          && (operations.contains("*") || operations.contains(event.getOperationType().name()));
    }

    boolean matches(AdminEvent event, Boolean enabled) {
      if (!matches(event)) {
        return false;
      }
      if (userEnabled == null) {
        return true;
      }
      boolean successful = event.getError() == null;
      boolean directUser = AffectedUser.directUserId(event) != null;
      boolean enabledMatches = userEnabled.equals(enabled);
      return successful && directUser && enabledMatches;
    }
  }

  private EventFilter(
      Set<String> userEvents,
      List<AdminRule> adminEvents,
      CaptureScope scope,
      List<DeliveryRule> deliveryRules,
      String sha256) {
    this.userEvents = Set.copyOf(userEvents);
    this.adminEvents = List.copyOf(adminEvents);
    this.scope = scope;
    this.deliveryRules = List.copyOf(deliveryRules);
    this.sha256 = sha256;
  }

  static EventFilter all() {
    return defaultFilter;
  }

  static EventFilter parse(byte[] bytes) throws IOException {
    JsonNode root = objectMapper.readTree(bytes);
    fields(
        root,
        Set.of(
            "userEvents",
            "adminEvents",
            "realmIds",
            "clientIds",
            "outcomes",
            "subjects",
            "delivery"));
    Set<String> users = names(root.get("userEvents"), EventType.class);
    JsonNode admins = root.get("adminEvents");
    require(admins != null && admins.isArray(), "adminEvents must be an array");
    List<AdminRule> rules = new ArrayList<>();
    for (JsonNode rule : admins) {
      fields(rule, Set.of("resourceType", "operations", "userEnabled"));
      rules.add(adminRule(rule));
    }
    String sha256 = digest(bytes);
    return new EventFilter(
        users, rules, CaptureScope.parse(root), deliveryRules(root, sha256), sha256);
  }

  private static AdminRule adminRule(JsonNode rule) {
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
    return new AdminRule(type, operations, enabled);
  }

  private static List<DeliveryRule> deliveryRules(JsonNode root, String sha256) {
    if (!root.has("delivery")) {
      return List.of();
    }
    JsonNode delivery = root.get("delivery");
    fields(delivery, Set.of("rules"));
    JsonNode rules = delivery.get("rules");
    require(rules != null && rules.isArray(), "delivery.rules must be an array");
    Set<String> ids = new HashSet<>();
    List<DeliveryRule> result = new ArrayList<>();
    for (JsonNode rule : rules) {
      fields(rule, Set.of("id", "match", "policy"));
      JsonNode id = rule.get("id");
      require(
          id != null && id.isTextual() && !id.textValue().isBlank(),
          "Rule ID must be a nonblank string");
      require(ids.add(id.textValue()), "Duplicate delivery rule ID");
      JsonNode match = rule.get("match");
      require(match != null && match.isObject(), "Rule match must be an object");
      JsonNode kind = match.get("kind");
      require(kind != null && kind.isTextual(), "Rule kind is required");
      Set<String> eventTypes = Set.of();
      AdminRule admin = null;
      switch (kind.textValue()) {
        case "user" -> {
          fields(
              match, Set.of("kind", "eventTypes", "realmIds", "clientIds", "outcomes", "subjects"));
          eventTypes = names(match.get("eventTypes"), EventType.class);
        }
        case "admin" -> {
          fields(
              match,
              Set.of(
                  "kind",
                  "resourceType",
                  "operations",
                  "userEnabled",
                  "realmIds",
                  "clientIds",
                  "outcomes",
                  "subjects"));
          admin = adminRule(match);
        }
        default -> throw new IllegalArgumentException("Unknown rule kind");
      }
      result.add(
          new DeliveryRule(
              eventTypes,
              admin,
              CaptureScope.parse(match),
              new ResolvedPublicationPolicy(
                  publicationPolicy(rule.get("policy")), sha256, id.textValue())));
    }
    return result;
  }

  private static PublicationPolicy publicationPolicy(JsonNode policy) {
    fields(policy, Set.of("action", "maxAgeSeconds", "maxFailures"));
    JsonNode action = policy.get("action");
    require(action != null && action.isTextual(), "Publication action is required");
    Integer age = limit(policy, "maxAgeSeconds");
    Integer failures = limit(policy, "maxFailures");
    switch (action.textValue()) {
      case "retry" -> require(age == null && failures == null, "retry forbids limits");
      case "discard" -> require(age != null || failures != null, "discard requires a limit");
      default -> throw new IllegalArgumentException("Unknown publication action");
    }
    return new PublicationPolicy(age, failures);
  }

  private static Integer limit(JsonNode policy, String field) {
    if (!policy.has(field)) {
      return null;
    }
    JsonNode value = policy.get(field);
    require(
        value.isIntegralNumber() && value.canConvertToInt(),
        "Publication limit must be an integer");
    return value.intValue();
  }

  String sha256() {
    return sha256;
  }

  Optional<ResolvedPublicationPolicy> resolve(Event event, String subject) {
    if (!accepts(event, subject)) {
      return Optional.empty();
    }
    return Optional.of(
        deliveryRules.stream()
            .filter(rule -> rule.matches(event, subject))
            .map(DeliveryRule::resolved)
            .findFirst()
            .orElseGet(this::defaultPolicy));
  }

  Optional<ResolvedPublicationPolicy> resolve(AdminEvent event, Boolean enabled, String subject) {
    if (!accepts(event, enabled, subject)) {
      return Optional.empty();
    }
    return Optional.of(
        deliveryRules.stream()
            .filter(rule -> rule.matches(event, enabled, subject))
            .map(DeliveryRule::resolved)
            .findFirst()
            .orElseGet(this::defaultPolicy));
  }

  private ResolvedPublicationPolicy defaultPolicy() {
    return new ResolvedPublicationPolicy(PublicationPolicy.RETRY, sha256, null);
  }

  boolean accepts(Event event, String subject) {
    return (userEvents.contains("*") || userEvents.contains(event.getType().name()))
        && scope.accepts(event.getRealmId(), event.getClientId(), event.getError(), subject);
  }

  boolean accepts(AdminEvent event, Boolean enabled, String subject) {
    return inScope(scope, event, subject)
        && adminEvents.stream().anyMatch(rule -> rule.matches(event, enabled));
  }

  boolean mayAccept(AdminEvent event, String subject) {
    return inScope(scope, event, subject)
        && adminEvents.stream().anyMatch(rule -> rule.matches(event));
  }

  private static boolean inScope(CaptureScope scope, AdminEvent event, String subject) {
    String client = event.getAuthDetails() == null ? null : event.getAuthDetails().getClientId();
    return scope.accepts(event.getRealmId(), client, event.getError(), subject);
  }

  private static void fields(JsonNode node, Set<String> allowed) {
    require(node != null && node.isObject(), "Expected an object");
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

  static String digest(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}
