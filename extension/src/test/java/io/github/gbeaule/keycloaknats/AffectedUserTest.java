package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ContextNotActiveException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;

class AffectedUserTest {
  @ParameterizedTest
  @CsvSource({
    "USER,users/target",
    "USER,users/target/credentials/credential",
    "USER,users/target/reset-password",
    "USER,users/target/logout",
    "USER,users/target/execute-actions-email",
    "USER,users/target/send-verify-email",
    "USER,users/target/reset-password-email",
    "USER,users/target/consents/client",
    "USER_LOGIN_FAILURE,users/target",
    "GROUP_MEMBERSHIP,users/target/groups/group",
    "REALM_ROLE_MAPPING,users/target/role-mappings/realm",
    "CLIENT_ROLE_MAPPING,users/target/role-mappings/clients/client"
  })
  void emittedPathsIdentifyTheAffectedUserEvenOnErrors(String resource, String path)
      throws Exception {
    var event = EventEnvelopeTest.admin(OperationType.DELETE);
    if (resource.equals("USER") && !path.equals("users/target")) {
      event.setOperationType(OperationType.ACTION);
    }
    event.setResourceTypeAsString(resource);
    event.setResourcePath(path);
    event.setError("denied");
    event.setAuthDetails(new AuthDetails());
    event.getAuthDetails().setUserId("actor");
    event.getAuthDetails().setRealmId("master");
    var session = request(event, "target");
    String userId = AffectedUser.resolve(event, session);
    assertEquals("target", userId);
    var encoder = new EventEnvelope(BridgeConfig.from(Map.of()));
    var row =
        encoder.serialize(
            UUID.randomUUID().toString(),
            encoder.describe(event, userId, null, null),
            new EventOrdering("realm", "target", 2),
            123,
            CaptureFixtures.RETRY);
    var data = new ObjectMapper().readTree(row.payload()).path("data");
    assertEquals("target", data.path("userId").asText());
    assertEquals("actor", data.path("actorUserId").asText());
    assertEquals("realm", data.path("realmId").asText());
    assertFalse(data.has("userEnabled"));
  }

  @ParameterizedTest
  @CsvSource({
    "USER,users/target/roles",
    "USER,users/target/credentials",
    "USER,users/target/credentials/c/userLabel",
    "USER,users/target/groups/g",
    "USER,users/target/role-mappings/realm",
    "USER,users/target/role-mappings",
    "GROUP_MEMBERSHIP,users/target",
    "GROUP_MEMBERSHIP,users/target/groups",
    "GROUP_MEMBERSHIP,users/target/consents/c",
    "GROUP_MEMBERSHIP,users/target/groups/g/extra",
    "REALM_ROLE_MAPPING,users/target",
    "REALM_ROLE_MAPPING,users/target/other/realm",
    "REALM_ROLE_MAPPING,users/target/role-mappings/clients",
    "CLIENT_ROLE_MAPPING,users/target",
    "CLIENT_ROLE_MAPPING,users/target/other/clients/c",
    "CLIENT_ROLE_MAPPING,users/target/role-mappings/realm/c",
    "CLIENT_ROLE_MAPPING,groups/target/role-mappings/clients/c",
    "custom:USER,users/target",
    ",users/target",
    "USER,",
    "USER,users",
    "USER,users/",
    "USER,users/ /credentials/c",
    "USER,users//credentials/c",
    "USER,/users/target",
    "USER,users/./credentials/c",
    "USER,users/../credentials/c",
    "USER,users/target/credentials/"
  })
  void unrelatedCustomAndMalformedPathsStayIndependent(String resource, String path) {
    var event = EventEnvelopeTest.admin(OperationType.ACTION);
    event.setResourceTypeAsString(resource);
    event.setResourcePath(path);
    assertNull(AffectedUser.resolve(event));
    assertNull(AffectedUser.directUserId(event));
    assertNull(AffectedUser.resolve(event, request(event, "target")));
  }

  @ParameterizedTest
  @CsvSource({
    "USER,UPDATE,users/target/",
    "USER,ACTION,users/target/logout/",
    "USER,ACTION,users/target/reset-password/",
    "USER,ACTION,users/target/execute-actions-email/",
    "USER,ACTION,users/target/send-verify-email/",
    "USER,ACTION,users/target/reset-password-email/",
    "USER,ACTION,users/target/credentials/credential/",
    "USER,ACTION,users/target/consents/client/",
    "USER,CREATE,users/target/federated-identity/provider/",
    "USER,DELETE,users/target/federated-identity/provider/",
    "USER_LOGIN_FAILURE,DELETE,users/target/",
    "GROUP_MEMBERSHIP,CREATE,users/target/groups/group/",
    "REALM_ROLE_MAPPING,DELETE,users/target/role-mappings/realm/",
    "CLIENT_ROLE_MAPPING,CREATE,users/target/role-mappings/clients/client/"
  })
  void endpointTrailingSlashesPreserveRoutedIdentity(
      String resource, String operation, String path) {
    var event = EventEnvelopeTest.admin(OperationType.valueOf(operation));
    event.setResourceTypeAsString(resource);
    event.setResourcePath(path);
    assertEquals("target", AffectedUser.resolve(event, request(event, "target")));
    assertEquals(
        path.equals("users/target/") && resource.equals("USER"),
        AffectedUser.isDirectUser(event, "target"));
    assertNull(
        AffectedUser.resolve(event), "Decoded trailing slashes alone cannot identify a user");
  }

  @ParameterizedTest
  @ValueSource(strings = {"f:p:x/", "f:p:x//", "f:p:x/+%2F/用户/"})
  void trailingDelimiterDoesNotTrimOpaqueRoutedUserIds(String user) {
    var event = EventEnvelopeTest.admin(OperationType.UPDATE);
    event.setResourcePath("users/" + user + "/");
    assertEquals(user, AffectedUser.resolve(event, request(event, user)));
    assertTrue(AffectedUser.isDirectUser(event, user));
    assertNull(AffectedUser.resolve(event));
    event.setOperationType(OperationType.ACTION);
    event.setResourcePath("users/" + user + "/logout/");
    assertEquals(user, AffectedUser.resolve(event, request(event, user)));
    assertFalse(AffectedUser.isDirectUser(event, user));
  }

  @ParameterizedTest
  @ValueSource(strings = {"CREATE", "DELETE"})
  void federatedIdentityRoutesRequireTheirExactOpaqueProvider(String operation) {
    String user = "f:p:x/federated-identity";
    String provider = "provider/+%2F/用户/";
    var event = EventEnvelopeTest.admin(OperationType.valueOf(operation));
    event.setResourcePath("users/" + user + "/federated-identity/" + provider);
    var session = AdminRequestFixtures.request(event, user, Map.of("provider", provider));
    assertEquals(user, AffectedUser.resolve(event, session));
    assertFalse(AffectedUser.isDirectUser(event, user));
    assertNull(AffectedUser.resolve(event));
    session.getContext().getUri().getPathParameters().putSingle("provider", "other");
    assertNull(AffectedUser.resolve(event, session));
    session.getContext().getUri().getPathParameters().remove("provider");
    assertNull(AffectedUser.resolve(event, session));
  }

  @ParameterizedTest
  @CsvSource({
    "USER,UPDATE,users/target/logout",
    "USER,DELETE,users/target/send-verify-email",
    "USER,UPDATE,users/target/federated-identity/provider",
    "USER,ACTION,users/target/federated-identity/provider",
    "USER_LOGIN_FAILURE,UPDATE,users/target",
    "USER_LOGIN_FAILURE,DELETE,users/target/logout",
    "USER,ACTION,users/target/logout//"
  })
  void nestedResourcesRejectUnrelatedOperationsAndExtraDelimiters(
      String resource, String operation, String path) {
    var event = EventEnvelopeTest.admin(OperationType.valueOf(operation));
    event.setResourceTypeAsString(resource);
    event.setResourcePath(path);
    assertNull(AffectedUser.resolve(event, request(event, "target")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"f:provider:external-id", "f:provider:用户+%252F", "literal%2Fid"})
  void storageIdsArePreservedWithoutUrlDecoding(String id) {
    var event = EventEnvelopeTest.admin(OperationType.DELETE);
    event.setResourcePath("users/" + id);
    assertEquals(id, AffectedUser.resolve(event));
    assertEquals(id, AffectedUser.directUserId(event));
  }

  @Test
  void controlCharactersDoNotCreateAnAttributablePath() {
    var event = EventEnvelopeTest.admin(OperationType.DELETE);
    event.setResourcePath("users/target\u0000/credentials/c");
    assertNull(AffectedUser.resolve(event));
  }

  @ParameterizedTest
  @ValueSource(strings = {"CREATE", "UPDATE", "DELETE"})
  void decodedStorageIdsContainingSlashesCannotImpersonateNestedActions(String operation) {
    var event = EventEnvelopeTest.admin(OperationType.valueOf(operation));
    event.setResourcePath("users/f:provider:external/credentials/id");
    assertNull(AffectedUser.resolve(event));
    assertNull(AffectedUser.resolve(event, request(event, "f:provider:external")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"f:p:x/credentials", "f:p:x/consents", "f:p:x/+%2F/用户", "f:p:x//part"})
  void routedOpaqueIdsAreNotSplitTrimmedOrDecodedAgain(String id) {
    var event = EventEnvelopeTest.admin(OperationType.ACTION);
    event.setResourcePath("users/" + id + "/reset-password");
    assertEquals(id, AffectedUser.resolve(event, request(event, id)));
    assertFalse(AffectedUser.isDirectUser(event, id));
    assertNull(AffectedUser.resolve(event));
    event.setOperationType(OperationType.UPDATE);
    event.setResourcePath("users/" + id);
    assertEquals(id, AffectedUser.resolve(event, request(event, id)));
    assertTrue(AffectedUser.isDirectUser(event, id));
  }

  @ParameterizedTest
  @CsvSource({
    "USER,/credentials/,credentialId",
    "USER,/consents/,client",
    "GROUP_MEMBERSHIP,/groups/,groupId",
    "CLIENT_ROLE_MAPPING,/role-mappings/clients/,client-id"
  })
  void nestedParametersAreAlsoOpaque(String resource, String suffix, String parameter) {
    String user = "f:p:x/consents";
    String child = "nested/+%2F/用户";
    var event = EventEnvelopeTest.admin(OperationType.ACTION);
    event.setResourceTypeAsString(resource);
    event.setResourcePath("users/" + user + suffix + child);
    var session = AdminRequestFixtures.request(event, user, Map.of(parameter, child));
    assertEquals(user, AffectedUser.resolve(event, session));
    assertFalse(AffectedUser.isDirectUser(event, user));
    assertNull(AffectedUser.resolve(event));
  }

  @Test
  void identicalDecodedPathsResolveToDifferentUsersFromTheirRoutes() {
    var event = EventEnvelopeTest.admin(OperationType.ACTION);
    event.setResourcePath("users/f:p:x/consents/reset-password");
    assertEquals("f:p:x/consents", AffectedUser.resolve(event, request(event, "f:p:x/consents")));
    assertEquals(
        "f:p:x",
        AffectedUser.resolve(
            event,
            AdminRequestFixtures.request(event, "f:p:x", Map.of("client", "reset-password"))));
    assertNull(AffectedUser.resolve(event));
    assertNull(AffectedUser.resolve(event, mock(KeycloakSession.class)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "realm",
        "path",
        "uri",
        "missing-realm",
        "inactive",
        "missing-user",
        "different-user"
      })
  void unrelatedOrMissingRequestContextCannotSupplyNestedIdentity(String mismatch) {
    var event = EventEnvelopeTest.admin(OperationType.ACTION);
    event.setResourcePath("users/target/reset-password");
    var session = request(event, "target");
    var context = session.getContext();
    var uri = context.getUri();
    switch (mismatch) {
      case "realm" -> when(context.getRealm().getId()).thenReturn("another-realm");
      case "path" ->
          when(uri.getPath()).thenReturn("/admin/realms/realm-name/users/other/reset-password");
      case "uri" -> when(context.getUri()).thenReturn(null);
      case "missing-realm" -> when(context.getRealm()).thenReturn(null);
      case "inactive" -> when(uri.getPathParameters()).thenThrow(new ContextNotActiveException());
      case "missing-user" -> uri.getPathParameters().remove("user-id");
      case "different-user" -> uri.getPathParameters().putSingle("user-id", "other");
      default -> throw new AssertionError(mismatch);
    }
    assertNull(AffectedUser.resolve(event, session));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t", "target\u0000"})
  void invalidRoutedIdsDoNotFallBackToGuessingNestedUsers(String id) {
    var event = EventEnvelopeTest.admin(OperationType.ACTION);
    event.setResourcePath("users/target/reset-password");
    var session = request(event, "target");
    session.getContext().getUri().getPathParameters().put("user-id", Collections.singletonList(id));
    assertNull(AffectedUser.resolve(event, session));
  }

  @Test
  void duplicateOrEmptyRouteValuesAreNotGuessed() {
    var event = EventEnvelopeTest.admin(OperationType.ACTION);
    event.setResourcePath("users/target/credentials/credential");
    var session = request(event, "target");
    var parameters = session.getContext().getUri().getPathParameters();
    parameters.put("user-id", List.of("target", "another"));
    assertNull(AffectedUser.resolve(event, session));
    parameters.put("user-id", List.of());
    assertNull(AffectedUser.resolve(event, session));
    parameters.put("user-id", List.of("target"));
    parameters.remove("credentialId");
    assertNull(AffectedUser.resolve(event, session));
  }

  @Test
  void directUserFallbackWorksWithoutAnActiveRequest() {
    var event = EventEnvelopeTest.admin(OperationType.CREATE);
    var session = request(event, null);
    assertEquals("target", AffectedUser.resolve(event, session));
    when(session.getContext().getUri()).thenThrow(new ContextNotActiveException());
    assertEquals("target", AffectedUser.resolve(event, session));
    assertEquals("target", AffectedUser.resolve(event, mock(KeycloakSession.class)));
  }

  @Test
  void missingRequestPathCannotBorrowIdentity() {
    var event = EventEnvelopeTest.admin(OperationType.ACTION);
    event.setResourcePath("users/target/reset-password");
    var session = request(event, "target");
    when(session.getContext().getUri().getPath()).thenReturn(null);
    assertNull(AffectedUser.resolve(event, session));
  }

  private static KeycloakSession request(
      org.keycloak.events.admin.AdminEvent event, String userId) {
    return AdminRequestFixtures.request(
        event,
        userId,
        Map.of(
            "credentialId",
            "credential",
            "client",
            "client",
            "groupId",
            "group",
            "client-id",
            "client",
            "provider",
            "provider"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void blankUserEventsAreIndependent(String id) {
    var event = EventEnvelopeTest.login();
    event.setUserId(id);
    assertNull(AffectedUser.resolve(event));
  }
}
