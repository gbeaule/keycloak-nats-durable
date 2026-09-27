package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;

class DeliveryRulesTest {
  static final String USER_MATCH = "{\"kind\":\"user\",\"eventTypes\":[\"LOGIN\"]}";
  static final String ADMIN_MATCH =
      "{\"kind\":\"admin\",\"resourceType\":\"USER\",\"operations\":[\"UPDATE\"]}";
  private static final EventEnvelope envelopes = new EventEnvelope(BridgeConfig.from(Map.of()));

  static String rule(String id, String match, String policy) {
    return "{\"id\":\"" + id + "\",\"match\":" + match + ",\"policy\":" + policy + "}";
  }

  static String document(String... rules) {
    return "{\"userEvents\":[\"*\"],\"adminEvents\":[{\"resourceType\":\"*\","
        + "\"operations\":[\"*\"]}],\"delivery\":{\"rules\":["
        + String.join(",", rules)
        + "]}}";
  }

  static ResolvedPublicationPolicy resolve(EventFilter filter, Event event) {
    return filter.resolve(event, envelopes.userSubject(event)).orElseThrow();
  }

  static ResolvedPublicationPolicy resolve(EventFilter filter, AdminEvent event, Boolean enabled) {
    return filter.resolve(event, enabled, envelopes.adminSubject(event)).orElseThrow();
  }

  @Test
  void firstMatchWinsWithoutMergingAndDefaultHasExactByteProvenance() throws Exception {
    String json =
        document(
            rule("retain", "{\"kind\":\"user\",\"eventTypes\":[\"*\"]}", "{\"action\":\"retry\"}"),
            rule("login", USER_MATCH, "{\"action\":\"discard\",\"maxFailures\":1}"));
    var filter = EventFilterTest.parse(json);
    var result = resolve(filter, EventEnvelopeTest.login());
    assertEquals("retain", result.ruleId());
    assertEquals(PublicationPolicy.RETRY, result.policy());
    String digest =
        HexFormat.of()
            .formatHex(
                MessageDigest.getInstance("SHA-256").digest(json.getBytes(StandardCharsets.UTF_8)));
    assertEquals(digest, result.filterSha256());
    assertEquals(digest, filter.sha256());
    var adminDefault = resolve(filter, EventEnvelopeTest.admin(OperationType.DELETE), null);
    assertNull(adminDefault.ruleId());
    assertEquals(PublicationPolicy.RETRY, adminDefault.policy());
    assertEquals(digest, adminDefault.filterSha256());
    assertNotEquals(digest, EventFilterTest.parse(json + " ").sha256());
    assertEquals(
        PublicationPolicy.RETRY, resolve(EventFilter.all(), EventEnvelopeTest.login()).policy());
  }

  @Test
  void captureExclusionsAlwaysTakePrecedenceOverDelivery() throws Exception {
    String json =
        document(
            rule("login", USER_MATCH, "{\"action\":\"discard\",\"maxAgeSeconds\":1}"),
            rule("admin", ADMIN_MATCH, "{\"action\":\"discard\",\"maxFailures\":1}"));
    var filter =
        EventFilterTest.parse(
            json.replace("\"userEvents\":[\"*\"]", "\"userEvents\":[]")
                .replace("\"operations\":[\"*\"]", "\"operations\":[]"));
    assertTrue(filter.resolve(EventEnvelopeTest.login(), "anything").isEmpty());
    var admin = EventEnvelopeTest.admin(OperationType.UPDATE);
    assertFalse(filter.mayAccept(admin, envelopes.adminSubject(admin)));
    assertTrue(filter.resolve(admin, false, envelopes.adminSubject(admin)).isEmpty());
  }

  @Test
  void userDeliveryScopesAndUnmatchedEventsKeepTheirOwnPolicies() throws Exception {
    String match =
        """
        {"kind":"user","eventTypes":["LOGIN","LOGIN_ERROR"],"realmIds":["realm"],
         "clientIds":["app"],"outcomes":["success"],"subjects":["keycloak.events.*.user.login"]}
        """;
    var filter =
        EventFilterTest.parse(
            document(
                rule(
                    "scoped",
                    match,
                    "{\"action\":\"discard\",\"maxAgeSeconds\":300,\"maxFailures\":20}")));
    var event = EventEnvelopeTest.login();
    event.setRealmId("realm");
    event.setClientId("app");
    var selected = resolve(filter, event);
    assertEquals("scoped", selected.ruleId());
    assertEquals(new PublicationPolicy(300, 20), selected.policy());
    event.setType(EventType.LOGOUT);
    assertNull(resolve(filter, event).ruleId());
    event.setType(EventType.LOGIN_ERROR);
    assertNull(resolve(filter, event).ruleId());
    event.setType(EventType.LOGIN);
    event.setError("failed");
    assertNull(resolve(filter, event).ruleId());
    event.setError(null);
    event.setClientId(null);
    assertNull(resolve(filter, event).ruleId());
    event.setClientId("app");
    event.setRealmId("other");
    assertNull(resolve(filter, event).ruleId());
  }

  @Test
  void adminRulesReuseScopeCustomResourcesAndDirectSuccessfulStateObservations() throws Exception {
    String match =
        """
        {"kind":"admin","resourceType":"USER","operations":["CREATE","UPDATE"],
         "userEnabled":false,"realmIds":["realm"],"clientIds":["app"]}
        """;
    var filter =
        EventFilterTest.parse(
            document(
                rule("user-rule", USER_MATCH, "{\"action\":\"retry\"}"),
                rule("disabled", match, "{\"action\":\"discard\",\"maxFailures\":2}"),
                rule(
                    "custom",
                    "{\"kind\":\"admin\",\"resourceType\":\"custom:widget\","
                        + "\"operations\":[\"*\"]}",
                    "{\"action\":\"discard\",\"maxAgeSeconds\":1}")));
    var event = EventEnvelopeTest.admin(OperationType.UPDATE);
    event.setAuthDetails(new AuthDetails());
    event.getAuthDetails().setClientId("app");
    assertTrue(filter.mayAccept(event, envelopes.adminSubject(event)));
    assertEquals("disabled", resolve(filter, event, false).ruleId());
    assertNull(resolve(filter, event, true).ruleId());
    assertNull(resolve(filter, event, null).ruleId());
    event.setError("failed");
    assertNull(resolve(filter, event, false).ruleId());
    event.setError(null);
    event.setResourcePath("users/target/credentials");
    assertNull(resolve(filter, event, false).ruleId());
    event.setResourcePath("users/target");
    event.setRealmId("other");
    assertNull(resolve(filter, event, false).ruleId());
    event.setResourceTypeAsString("widget");
    assertEquals("custom", resolve(filter, event, null).ruleId());
    assertEquals(PublicationPolicy.RETRY, resolve(filter, EventEnvelopeTest.login()).policy());
  }

  @Test
  void rejectsDuplicateIdsAndRawJsonFieldsAtEveryDepth() {
    String rule = rule("same", USER_MATCH, "{\"action\":\"retry\"}");
    assertThrows(IllegalArgumentException.class, () -> EventFilterTest.parse(document(rule, rule)));
    String other = rule("same", ADMIN_MATCH, "{\"action\":\"discard\",\"maxAgeSeconds\":1}");
    assertThrows(
        IllegalArgumentException.class, () -> EventFilterTest.parse(document(rule, other)));
    for (String duplicate :
        new String[] {
          rule.replace("\"id\":", "\"id\":\"other\",\"id\":"),
          rule.replace("\"kind\":", "\"kind\":\"user\",\"kind\":"),
          rule.replace("\"action\":", "\"action\":\"discard\",\"action\":")
        }) {
      assertThrows(JsonProcessingException.class, () -> EventFilterTest.parse(document(duplicate)));
    }
    assertThrows(
        JsonProcessingException.class,
        () -> EventFilterTest.parse(document().replace("\"rules\":", "\"rules\":[],\"rules\":")));
  }
}
