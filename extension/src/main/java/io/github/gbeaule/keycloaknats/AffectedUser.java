package io.github.gbeaule.keycloaknats;

import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.ws.rs.core.MultivaluedMap;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.keycloak.events.Event;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;

/** Uses routed IDs for admin requests; decoded paths alone cannot identify nested users safely. */
final class AffectedUser {
  private static final String BRUTE_FORCE_USERS = "attack-detection/brute-force/users/";
  private static final String ORGANIZATION_MEMBERSHIP = "ORGANIZATION_MEMBERSHIP";
  private static final List<String> USER_ACTIONS =
      List.of(
          "reset-password",
          "logout",
          "execute-actions-email",
          "send-verify-email",
          "reset-password-email");

  private AffectedUser() {}

  static String resolve(Event event) {
    String id = event.getUserId();
    return id == null || id.isBlank() ? null : id;
  }

  static String resolve(AdminEvent event) {
    return directUserId(event);
  }

  static String resolve(AdminEvent event, KeycloakSession session) {
    var context = session.getContext();
    if (context == null) {
      return resolve(event);
    }
    try {
      var uri = context.getUri();
      var realm = context.getRealm();
      if (uri != null && realm != null) {
        var parameters = uri.getPathParameters();
        String resourcePath = event.getResourcePath();
        // Organization members and brute-force resets use their own routed user parameters.
        boolean organizationMembership =
            ORGANIZATION_MEMBERSHIP.equals(event.getResourceTypeAsString());
        boolean bruteForceUserPath =
            resourcePath != null && resourcePath.startsWith(BRUTE_FORCE_USERS);
        String userIdParameter = "user-id";
        if (organizationMembership) {
          userIdParameter = "member-id";
        } else if (bruteForceUserPath) {
          userIdParameter = "userId";
        }
        String userId = parameter(parameters, userIdParameter);
        String requestPath = uri.getPath();
        if (userId != null
            && resourcePath != null
            && requestPath != null
            && Objects.equals(realm.getId(), event.getRealmId())
            && requestPath.endsWith("/realms/" + realm.getName() + "/" + resourcePath)) {
          return matchesResource(event, userId, parameters) ? userId : null;
        }
      }
    } catch (ContextNotActiveException noRequest) {
      // Background callbacks can still identify an unambiguous direct user from their event.
    }
    return resolve(event);
  }

  private static boolean matchesResource(
      AdminEvent event, String userId, MultivaluedMap<String, String> parameters) {
    String resource = event.getResourceTypeAsString();
    String path = event.getResourcePath();
    String base = "users/" + userId;
    return switch (resource == null ? "" : resource) {
      case "USER" ->
          matchesPath(path, base)
              || (event.getOperationType() == OperationType.ACTION
                  && (USER_ACTIONS.stream()
                          .anyMatch(action -> matchesPath(path, base + "/" + action))
                      || matchesParameter(path, base + "/credentials/", parameters, "credentialId")
                      || matchesParameter(path, base + "/consents/", parameters, "client")))
              || ((event.getOperationType() == OperationType.CREATE
                      || event.getOperationType() == OperationType.DELETE)
                  && matchesParameter(path, base + "/federated-identity/", parameters, "provider"));
      case "USER_LOGIN_FAILURE" ->
          event.getOperationType() == OperationType.DELETE
              && (matchesPath(path, base) || matchesPath(path, BRUTE_FORCE_USERS + userId));
      case "GROUP_MEMBERSHIP" -> matchesParameter(path, base + "/groups/", parameters, "groupId");
      case "REALM_ROLE_MAPPING" -> matchesPath(path, base + "/role-mappings/realm");
      case "CLIENT_ROLE_MAPPING" ->
          matchesParameter(path, base + "/role-mappings/clients/", parameters, "client-id");
      case ORGANIZATION_MEMBERSHIP ->
          event.getOperationType() == OperationType.DELETE
              && matchesOrganizationMember(path, userId, parameters);
      default -> false;
    };
  }

  private static boolean matchesOrganizationMember(
      String path, String userId, MultivaluedMap<String, String> parameters) {
    String organizationId = parameter(parameters, "org-id");
    return organizationId != null
        && matchesPath(path, "organizations/" + organizationId + "/members/" + userId);
  }

  private static boolean matchesParameter(
      String path, String prefix, MultivaluedMap<String, String> parameters, String name) {
    String value = parameter(parameters, name);
    return value != null && matchesPath(path, prefix + value);
  }

  private static boolean matchesPath(String path, String routedPath) {
    // Compare against the exact routed IDs first; a final slash can be an endpoint delimiter or
    // part of an opaque ID. Never remove slashes from decoded identities or guess without a route.
    return routedPath.equals(path) || (routedPath + "/").equals(path);
  }

  private static String parameter(MultivaluedMap<String, String> parameters, String name) {
    var values = parameters.get(name);
    if (values == null || values.size() != 1) {
      return null;
    }
    String value = values.getFirst();
    // Parameters are already decoded by Keycloak. Keep slashes, percent signs and '+' intact.
    return value == null || value.isBlank() || value.chars().anyMatch(Character::isISOControl)
        ? null
        : value;
  }

  static boolean isDirectUser(AdminEvent event, String userId) {
    return userId != null
        && "USER".equals(event.getResourceTypeAsString())
        && matchesPath(event.getResourcePath(), "users/" + userId);
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
