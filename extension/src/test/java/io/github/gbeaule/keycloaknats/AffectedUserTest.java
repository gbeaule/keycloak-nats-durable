package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;

class AffectedUserTest {
  @ParameterizedTest
  @CsvSource({
    "USER,users/target",
    "USER,users/target/credentials/credential",
    "USER,users/target/reset-password",
    "USER,users/target/consents/client",
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
    assertEquals("target", AffectedUser.resolve(event));
    var encoder = new EventEnvelope(BridgeConfig.from(Map.of()));
    var row =
        encoder.serialize(
            encoder.describe(event, null),
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
    "USER,users/target/",
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
