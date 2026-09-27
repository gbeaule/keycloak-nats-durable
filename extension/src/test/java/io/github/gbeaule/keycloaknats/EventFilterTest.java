package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;

class EventFilterTest {
  private static final EventEnvelope envelopes = new EventEnvelope(BridgeConfig.from(Map.of()));

  static boolean accepts(EventFilter policy, Event event) {
    return policy.accepts(event, envelopes.userSubject(event));
  }

  static boolean accepts(EventFilter policy, AdminEvent event, Boolean enabled) {
    return policy.accepts(event, enabled, envelopes.adminSubject(event));
  }

  static EventFilter parse(String json) throws IOException {
    return EventFilter.parse(json.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void allowlistsUserEventsAndAdminOperationsIndependently() throws Exception {
    var policy =
        parse(
            """
            {"userEvents":["LOGIN_ERROR"],"adminEvents":[
              {"resourceType":"USER","operations":["DELETE"]}]}
            """);
    var user = EventEnvelopeTest.login();
    assertFalse(accepts(policy, user));
    user.setType(EventType.LOGIN_ERROR);
    assertTrue(accepts(policy, user));
    assertTrue(accepts(policy, EventEnvelopeTest.admin(OperationType.DELETE), null));
    assertFalse(accepts(policy, EventEnvelopeTest.admin(OperationType.CREATE), true));
  }

  @Test
  void disabledOnlyRequiresSuccessfulDirectUserObservation() throws Exception {
    var policy =
        parse(
            """
            {"userEvents":[],"adminEvents":[
              {"resourceType":"USER","operations":["UPDATE"],"userEnabled":false}]}
            """);
    var admin = EventEnvelopeTest.admin(OperationType.UPDATE);
    assertTrue(accepts(policy, admin, false));
    assertFalse(accepts(policy, admin, true));
    assertFalse(accepts(policy, admin, null));
    admin.setError("failed");
    assertFalse(accepts(policy, admin, false));
    admin.setError(null);
    admin.setResourcePath("users/id/role-mappings");
    assertFalse(accepts(policy, admin, false));
  }

  @Test
  void emptyListsDisableCaptureAndWildcardPreservesAllEvents() throws Exception {
    var none = parse("{\"userEvents\":[],\"adminEvents\":[]}");
    assertFalse(accepts(none, EventEnvelopeTest.login()));
    assertFalse(
        none.mayAccept(
            EventEnvelopeTest.admin(OperationType.UPDATE),
            "keycloak.events.test.admin.user.update"));
    var all =
        parse(
            """
            {"userEvents":["*"],"adminEvents":[{"resourceType":"*","operations":["*"]}]}
            """);
    assertTrue(accepts(all, EventEnvelopeTest.login()));
    assertTrue(accepts(all, EventEnvelopeTest.admin(OperationType.DELETE), null));
  }

  @Test
  void supportsExplicitCustomResources() throws Exception {
    var policy =
        parse(
            """
            {"userEvents":[],"adminEvents":[{"resourceType":"custom:widget","operations":["*"]}]}
            """);
    var admin = EventEnvelopeTest.admin(OperationType.CREATE);
    admin.setResourceTypeAsString("widget");
    assertTrue(accepts(policy, admin, null));
    admin.setResourceTypeAsString("other-widget");
    assertFalse(accepts(policy, admin, null));
    assertFalse(policy.mayAccept(admin, envelopes.adminSubject(admin)));
  }

  @Test
  void scopesBothEventKindsByTargetRealmClientOutcomeAndSubject() throws Exception {
    var policy =
        parse(
            """
            {"realmIds":["selected"],"clientIds":["app"],"outcomes":["success"],
             "subjects":["keycloak.events.*.user.login","keycloak.events.*.admin.user.*"],
             "userEvents":["*"],"adminEvents":[{"resourceType":"*","operations":["*"]}]}
            """);
    var user = EventEnvelopeTest.login();
    user.setRealmId("selected");
    user.setClientId("app");
    assertTrue(accepts(policy, user));
    user.setType(EventType.LOGOUT);
    assertFalse(accepts(policy, user));
    user.setType(EventType.LOGIN);
    user.setRealmId("another");
    assertFalse(accepts(policy, user));
    user.setRealmId("selected");
    user.setClientId("different");
    assertFalse(accepts(policy, user));
    user.setClientId("app");
    user.setError("rejected");
    assertFalse(accepts(policy, user));

    var admin = EventEnvelopeTest.admin(OperationType.UPDATE);
    admin.setRealmId("selected");
    admin.setAuthDetails(new AuthDetails());
    admin.getAuthDetails().setClientId("app");
    assertTrue(accepts(policy, admin, false));
    admin.setRealmId("another");
    assertFalse(policy.mayAccept(admin, envelopes.adminSubject(admin)));
    admin.setRealmId("selected");
    admin.setAuthDetails(null);
    assertFalse(accepts(policy, admin, false));
  }

  @ParameterizedTest
  @ValueSource(strings = {"realmIds", "clientIds", "outcomes", "subjects"})
  void explicitEmptyScopeExcludesEverything(String dimension) throws Exception {
    var policy =
        parse(
            "{\""
                + dimension
                + "\":[],\"userEvents\":[\"*\"],"
                + "\"adminEvents\":[{\"resourceType\":\"*\",\"operations\":[\"*\"]}]}");
    assertFalse(accepts(policy, EventEnvelopeTest.login()));
    assertFalse(accepts(policy, EventEnvelopeTest.admin(OperationType.DELETE), null));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"realmIds\":null",
        "\"realmIds\":[\"\"]",
        "\"realmIds\":[\"*\",\"id\"]",
        "\"clientIds\":[1]",
        "\"outcomes\":[\"failed\"]",
        "\"subjects\":[\"events.>.login\"]",
        "\"subjects\":[\"events.*login\"]",
        "\"subjects\":[\"events..login\"]",
        "\"clientIds\":[\"app\",\"app\"]"
      })
  void rejectsMalformedScope(String scope) {
    assertThrows(
        IllegalArgumentException.class,
        () -> parse("{" + scope + ",\"userEvents\":[],\"adminEvents\":[]}"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{}",
        "null",
        "[]",
        "{\"userEvents\":[]}",
        "{\"userEvents\":[],\"adminEvents\":[],\"typo\":true}",
        "{\"userEvents\":[\"LGIN\"],\"adminEvents\":[]}",
        "{\"userEvents\":[true],\"adminEvents\":[]}",
        "{\"userEvents\":[\"*\",\"LOGIN\"],\"adminEvents\":[]}",
        "{\"userEvents\":[\"LOGIN\",\"LOGIN\"],\"adminEvents\":[]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"USRE\",\"operations\":[\"*\"]}]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"USER\","
            + "\"operations\":[\"DELETE\"],\"userEnabled\":false}]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"USER\","
            + "\"operations\":[\"UPDATE\"],\"userEnabled\":null}]}"
      })
  void rejectsAmbiguousOrMistypedPolicies(String json) {
    assertThrows(IllegalArgumentException.class, () -> parse(json));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{",
        "{\"userEvents\":[],\"userEvents\":[\"*\"],\"adminEvents\":[]}",
        "{\"userEvents\":[],\"adminEvents\":[]} {}"
      })
  void rejectsMalformedJsonDuplicateFieldsAndTrailingDocuments(String json) {
    assertThrows(JsonProcessingException.class, () -> parse(json));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "true",
        "{\"userEvents\":{},\"adminEvents\":[]}",
        "{\"userEvents\":[],\"adminEvents\":{}}",
        "{\"userEvents\":[],\"adminEvents\":[null]}",
        "{\"userEvents\":[],\"adminEvents\":[{}]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":1,\"operations\":[\"*\"]}]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"USER\"}]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"custom:\","
            + "\"operations\":[\"*\"]}]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"custom:*\","
            + "\"operations\":[\"*\"]}]}"
      })
  void rejectsMissingOrWronglyTypedRuleFields(String json) {
    assertThrows(IllegalArgumentException.class, () -> parse(json));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"resourceType\":\"CLIENT\",\"operations\":[\"UPDATE\"]",
        "\"resourceType\":\"USER\",\"operations\":[]",
        "\"resourceType\":\"USER\",\"operations\":[\"*\"]"
      })
  void statePredicatesRequireExplicitDirectUserMutationOperations(String rule) {
    assertThrows(
        IllegalArgumentException.class,
        () -> parse("{\"userEvents\":[],\"adminEvents\":[{" + rule + ",\"userEnabled\":true}]}"));
  }

  @Test
  void rulesAreAlternativesAndEmptyOperationListsNeverMatch() throws Exception {
    var policy =
        parse(
            """
            {"userEvents":[],"adminEvents":[
              {"resourceType":"CLIENT","operations":["*"]},
              {"resourceType":"USER","operations":[]},
              {"resourceType":"USER","operations":["UPDATE"],"userEnabled":true}]}
            """);
    var event = EventEnvelopeTest.admin(OperationType.UPDATE);
    assertTrue(policy.mayAccept(event, envelopes.adminSubject(event)));
    assertTrue(accepts(policy, event, true));
    assertFalse(accepts(policy, event, false));
    assertFalse(accepts(policy, EventEnvelopeTest.admin(OperationType.DELETE), null));
  }

  @Test
  void explicitScopeWildcardsAndSubjectAlternativesKeepTheirDifferentMeanings() throws Exception {
    var policy =
        parse(
            """
            {"realmIds":["*"],"clientIds":["*"],"outcomes":["*"],
             "subjects":["*","keycloak.events.*.user.login"],"userEvents":["*"],"adminEvents":[]}
            """);
    var login = EventEnvelopeTest.login();
    assertTrue(accepts(policy, login));
    login.setType(EventType.LOGOUT);
    assertFalse(accepts(policy, login));
  }
}
