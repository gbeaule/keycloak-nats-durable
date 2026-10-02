package io.github.gbeaule.keycloaknats;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.gbeaule.keycloaknats.routing.SubjectPattern;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Optional dimensions are ANDed with the user/admin capture rules. */
record CaptureScope(
    Set<String> realmIds,
    Set<String> clientIds,
    Set<String> outcomes,
    List<SubjectPattern> subjects) {
  CaptureScope {
    realmIds = Set.copyOf(realmIds);
    clientIds = Set.copyOf(clientIds);
    outcomes = Set.copyOf(outcomes);
    subjects = List.copyOf(subjects);
  }

  static CaptureScope all() {
    return new CaptureScope(
        Set.of("*"), Set.of("*"), Set.of("*"), List.of(new SubjectPattern(">")));
  }

  static CaptureScope parse(JsonNode root) {
    Set<String> outcomes = values(root, "outcomes", "*");
    if (!Set.of("*", "success", "error").containsAll(outcomes)) {
      throw new IllegalArgumentException("outcomes accepts success, error or a sole wildcard");
    }
    return new CaptureScope(
        values(root, "realmIds", "*"),
        values(root, "clientIds", "*"),
        outcomes,
        values(root, "subjects", ">").stream().map(SubjectPattern::new).toList());
  }

  boolean accepts(String realm, String client, String error, String subject) {
    return matches(realmIds, realm)
        && matches(clientIds, client)
        && matches(outcomes, error == null ? "success" : "error")
        && subjects.stream().anyMatch(pattern -> pattern.matches(subject));
  }

  private static boolean matches(Set<String> allowed, String value) {
    return allowed.contains("*") || (value != null && allowed.contains(value));
  }

  private static Set<String> values(JsonNode root, String field, String fallback) {
    if (!root.has(field)) {
      return Set.of(fallback);
    }
    JsonNode node = root.get(field);
    if (!node.isArray()) {
      throw new IllegalArgumentException(field + " must be an array");
    }
    Set<String> values = new HashSet<>();
    for (JsonNode entry : node) {
      if (!entry.isTextual() || entry.textValue().isBlank() || !values.add(entry.textValue())) {
        throw new IllegalArgumentException(field + " must contain unique, nonblank strings");
      }
    }
    if (!field.equals("subjects") && values.contains("*") && values.size() != 1) {
      throw new IllegalArgumentException("Wildcard must be the only value");
    }
    return Set.copyOf(values);
  }
}
