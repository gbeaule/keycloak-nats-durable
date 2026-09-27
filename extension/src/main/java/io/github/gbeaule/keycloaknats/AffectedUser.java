package io.github.gbeaule.keycloaknats;

import java.util.Arrays;
import org.keycloak.events.Event;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;

/** Attribution from Keycloak's event identity and explicitly supported admin resource paths. */
final class AffectedUser {
  private AffectedUser() {}

  static String resolve(Event event) {
    String id = event.getUserId();
    return id == null || id.isBlank() ? null : id;
  }

  static String resolve(AdminEvent event) {
    String[] path = path(event);
    if (path == null) {
      return null;
    }
    String resource = event.getResourceTypeAsString();
    boolean recognized =
        switch (resource == null ? "" : resource) {
          case "USER" -> {
            boolean directUser = path.length == 2;
            boolean credentialOrConsent =
                path.length == 4 && (path[2].equals("credentials") || path[2].equals("consents"));
            boolean passwordReset = path.length == 3 && path[2].equals("reset-password");
            boolean supportedAction =
                event.getOperationType() == OperationType.ACTION
                    && (credentialOrConsent || passwordReset);
            yield directUser || supportedAction;
          }
          case "GROUP_MEMBERSHIP" -> path.length == 4 && path[2].equals("groups");
          case "REALM_ROLE_MAPPING" ->
              path.length == 4 && path[2].equals("role-mappings") && path[3].equals("realm");
          case "CLIENT_ROLE_MAPPING" ->
              path.length == 5 && path[2].equals("role-mappings") && path[3].equals("clients");
          default -> false;
        };
    return recognized ? path[1] : null;
  }

  static String directUserId(AdminEvent event) {
    String[] path = path(event);
    return path != null && path.length == 2 && "USER".equals(event.getResourceTypeAsString())
        ? path[1]
        : null;
  }

  private static String[] path(AdminEvent event) {
    String value = event.getResourcePath();
    if (value == null) {
      return null;
    }
    // AdminEventBuilder uses decoded UriInfo paths. Decode neither percent escapes nor '+'.
    String[] parts = value.split("/", -1);
    boolean userResourcePath = parts.length >= 2 && parts[0].equals("users");
    if (!userResourcePath) {
      return null;
    }
    boolean malformedSegments =
        Arrays.stream(parts)
            .anyMatch(
                part -> {
                  boolean blank = part.isBlank();
                  boolean relative = part.equals(".") || part.equals("..");
                  boolean controlCharacters = part.chars().anyMatch(Character::isISOControl);
                  return blank || relative || controlCharacters;
                });
    return malformedSegments ? null : parts;
  }
}
