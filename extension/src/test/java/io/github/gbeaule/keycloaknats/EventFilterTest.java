package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.OperationType;

class EventFilterTest {
  static EventFilter parse(String json) throws Exception {
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
    assertFalse(policy.accepts(user));
    user.setType(EventType.LOGIN_ERROR);
    assertTrue(policy.accepts(user));
    assertTrue(policy.accepts(EventEnvelopeTest.admin(OperationType.DELETE), null));
    assertFalse(policy.accepts(EventEnvelopeTest.admin(OperationType.CREATE), true));
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
    assertTrue(policy.accepts(admin, false));
    assertFalse(policy.accepts(admin, true));
    assertFalse(policy.accepts(admin, null));
    admin.setError("failed");
    assertFalse(policy.accepts(admin, false));
    admin.setError(null);
    admin.setResourcePath("users/id/role-mappings");
    assertFalse(policy.accepts(admin, false));
  }

  @Test
  void emptyListsDisableCaptureAndWildcardPreservesAllEvents() throws Exception {
    var none = parse("{\"userEvents\":[],\"adminEvents\":[]}");
    assertFalse(none.accepts(EventEnvelopeTest.login()));
    assertFalse(none.mayAccept(EventEnvelopeTest.admin(OperationType.UPDATE)));
    var all =
        parse(
            """
        {"userEvents":["*"],"adminEvents":[{"resourceType":"*","operations":["*"]}]}
        """);
    assertTrue(all.accepts(EventEnvelopeTest.login()));
    assertTrue(all.accepts(EventEnvelopeTest.admin(OperationType.DELETE), null));
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
    assertTrue(policy.accepts(admin, null));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{}",
        "null",
        "[]",
        "{",
        "{\"userEvents\":[]}",
        "{\"userEvents\":[],\"adminEvents\":[],\"typo\":true}",
        "{\"userEvents\":[\"LGIN\"],\"adminEvents\":[]}",
        "{\"userEvents\":[true],\"adminEvents\":[]}",
        "{\"userEvents\":[\"*\",\"LOGIN\"],\"adminEvents\":[]}",
        "{\"userEvents\":[\"LOGIN\",\"LOGIN\"],\"adminEvents\":[]}",
        "{\"userEvents\":[],\"userEvents\":[\"*\"],\"adminEvents\":[]}",
        "{\"userEvents\":[],\"adminEvents\":[]} {}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"USRE\",\"operations\":[\"*\"]}]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"USER\","
            + "\"operations\":[\"DELETE\"],\"userEnabled\":false}]}",
        "{\"userEvents\":[],\"adminEvents\":[{\"resourceType\":\"USER\","
            + "\"operations\":[\"UPDATE\"],\"userEnabled\":null}]}"
      })
  void rejectsAmbiguousOrMistypedPolicies(String json) {
    assertThrows(Exception.class, () -> parse(json));
  }
}
