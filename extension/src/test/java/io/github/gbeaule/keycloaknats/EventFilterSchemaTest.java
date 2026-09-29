package io.github.gbeaule.keycloaknats;

import static io.github.gbeaule.keycloaknats.DeliveryRulesTest.ADMIN_MATCH;
import static io.github.gbeaule.keycloaknats.DeliveryRulesTest.USER_MATCH;
import static io.github.gbeaule.keycloaknats.DeliveryRulesTest.document;
import static io.github.gbeaule.keycloaknats.DeliveryRulesTest.rule;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.admin.OperationType;

class EventFilterSchemaTest {
  private static final Schema schema =
      SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
          .getSchema(
              EventFilterSchemaTest.class.getResourceAsStream("/schemas/event-filter.schema.json"));

  private static void assertAccepted(String json) {
    var errors = schema.validate(json, InputFormat.JSON);
    assertTrue(errors.isEmpty(), errors::toString);
    assertDoesNotThrow(() -> EventFilterTest.parse(json));
  }

  private static void assertRejected(String json) {
    assertFalse(schema.validate(json, InputFormat.JSON).isEmpty(), json);
    assertThrows(IllegalArgumentException.class, () -> EventFilterTest.parse(json));
  }

  @ParameterizedTest
  @ValueSource(strings = {"all", "scoped", "disabled-only", "with-delivery"})
  void shippedConfigurationsRemainValid(String name) throws Exception {
    try (var input = getClass().getResourceAsStream("/config/events-" + name + ".json")) {
      String json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      assertAccepted(json);
      if (!name.equals("with-delivery")) {
        var filter = EventFilterTest.parse(json);
        var event = EventEnvelopeTest.login();
        filter
            .resolve(event, "keycloak.events.realm.user.login")
            .ifPresent(policy -> assertEquals(PublicationPolicy.RETRY, policy.policy()));
      }
    }
  }

  @Test
  void exampleProtectsUserLifecycleWhileLoginPolicyIsExplicitlyDiscardable() throws Exception {
    try (var input = getClass().getResourceAsStream("/config/events-with-delivery.json")) {
      var filter = EventFilterTest.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8));
      for (var operation :
          new OperationType[] {OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE}) {
        var event = EventEnvelopeTest.admin(operation);
        var policy =
            filter
                .resolve(
                    event,
                    AffectedUser.resolve(event),
                    false,
                    "keycloak.events.realm.admin.user.update")
                .orElseThrow();
        assertEquals(PublicationPolicy.RETRY, policy.policy());
        assertEquals("protected-user-lifecycle", policy.ruleId());
      }
      var policy =
          filter
              .resolve(EventEnvelopeTest.login(), "keycloak.events.realm.user.login")
              .orElseThrow();
      assertEquals("short-lived-login", policy.ruleId());
      assertEquals(new PublicationPolicy(300, 20), policy.policy());
    }
  }

  @Test
  void schemaSelectorsTrackTheKeycloakCatalogue() throws Exception {
    var mapper = new ObjectMapper();
    var definitions =
        mapper
            .readTree(getClass().getResourceAsStream("/schemas/event-filter.schema.json"))
            .get("$defs");
    var catalogue =
        mapper.readTree(getClass().getResourceAsStream("/schemas/keycloak-catalogue-26.7.4.json"));
    assertEquals(
        catalogue.get("userEventTypes"), withoutWildcard(definitions.at("/eventTypes/items/enum")));
    assertEquals(
        catalogue.get("adminOperationTypes"),
        withoutWildcard(definitions.at("/operations/items/enum")));
    assertEquals(
        catalogue.get("adminResourceTypes"),
        withoutWildcard(definitions.at("/resourceType/anyOf/0/enum")));
  }

  private static JsonNode withoutWildcard(JsonNode node) {
    var result = ((ArrayNode) node).deepCopy();
    result.remove(0);
    return result;
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"action\":\"retry\"}",
        "{\"action\":\"discard\",\"maxAgeSeconds\":1}",
        "{\"action\":\"discard\",\"maxFailures\":1}",
        "{\"action\":\"discard\",\"maxAgeSeconds\":31536000,\"maxFailures\":1000000}"
      })
  void validPoliciesHaveIdenticalSchemaAndParserAcceptance(String policy) {
    assertAccepted(document(rule("rule", USER_MATCH, policy)));
    assertAccepted(document(rule("rule", ADMIN_MATCH, policy)));
    assertAccepted(document());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "null",
        "[]",
        "{}",
        "{\"action\":null}",
        "{\"action\":1}",
        "{\"action\":\"drop\"}",
        "{\"action\":\"discard\"}",
        "{\"action\":\"retry\",\"maxAgeSeconds\":1}",
        "{\"action\":\"retry\",\"maxFailures\":1}",
        "{\"action\":\"retry\",\"typo\":1}",
        "{\"action\":\"discard\",\"maxFailures\":null}",
        "{\"action\":\"discard\",\"maxFailures\":\"1\"}",
        "{\"action\":\"discard\",\"maxFailures\":true}",
        "{\"action\":\"discard\",\"maxFailures\":1.5}",
        "{\"action\":\"discard\",\"maxFailures\":0}",
        "{\"action\":\"discard\",\"maxFailures\":1000001}",
        "{\"action\":\"discard\",\"maxAgeSeconds\":0}",
        "{\"action\":\"discard\",\"maxAgeSeconds\":-1}",
        "{\"action\":\"discard\",\"maxAgeSeconds\":31536001}",
        "{\"action\":\"discard\",\"maxAgeSeconds\":9223372036854775808}"
      })
  void invalidPoliciesAreRejectedBySchemaAndParser(String policy) {
    assertRejected(document(rule("rule", USER_MATCH, policy)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "null",
        "[]",
        "{}",
        "{\"kind\":null}",
        "{\"kind\":42}",
        "{\"kind\":\"other\"}",
        "{\"kind\":\"user\"}",
        "{\"kind\":\"user\",\"eventTypes\":[\"LGIN\"]}",
        "{\"kind\":\"user\",\"eventTypes\":[\"*\",\"LOGIN\"]}",
        "{\"kind\":\"user\",\"eventTypes\":[\"LOGIN\",\"LOGIN\"]}",
        "{\"kind\":\"user\",\"eventTypes\":[\"*\"],\"userEnabled\":false}",
        "{\"kind\":\"admin\"}",
        "{\"kind\":\"admin\",\"resourceType\":\"USER\","
            + "\"operations\":[\"UPDATE\"],\"eventTypes\":[]}",
        "{\"kind\":\"admin\",\"resourceType\":\"USRE\",\"operations\":[\"*\"]}",
        "{\"kind\":\"admin\",\"resourceType\":\"custom:\",\"operations\":[\"*\"]}",
        "{\"kind\":\"admin\",\"resourceType\":\"custom:*\",\"operations\":[\"*\"]}",
        "{\"kind\":\"admin\",\"resourceType\":\"USER\","
            + "\"operations\":[\"DELETE\"],\"userEnabled\":false}"
      })
  void invalidSelectorsAreRejectedBySchemaAndParser(String match) {
    assertRejected(document(rule("rule", match, "{\"action\":\"retry\"}")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "null",
        "[]",
        "{}",
        "{\"rules\":null}",
        "{\"rules\":{}}",
        "{\"rules\":[null]}",
        "{\"rules\":[{}]}",
        "{\"rules\":[],\"typo\":true}"
      })
  void invalidDeliverySectionsAreRejectedBySchemaAndParser(String delivery) {
    assertRejected("{\"userEvents\":[],\"adminEvents\":[],\"delivery\":" + delivery + "}");
  }

  @ParameterizedTest
  @ValueSource(strings = {"null", "true", "\"\"", "\" \\t\""})
  void ruleIdsMustBeNonblankStrings(String id) {
    assertRejected(
        document(
            rule("rule", USER_MATCH, "{\"action\":\"retry\"}")
                .replace("\"id\":\"rule\"", "\"id\":" + id)));
  }

  @Test
  void everyRequiredRuleFieldIsCheckedAndUnknownFieldsFailClosed() {
    assertRejected(document("{\"match\":" + USER_MATCH + ",\"policy\":{\"action\":\"retry\"}}"));
    assertRejected(document("{\"id\":\"rule\",\"policy\":{\"action\":\"retry\"}}"));
    assertRejected(document("{\"id\":\"rule\",\"match\":" + USER_MATCH + "}"));
    assertRejected(
        document(
            rule("rule", USER_MATCH, "{\"action\":\"retry\"}")
                .replace("\"id\":", "\"unknown\":true,\"id\":")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"realmIds\":[\"*\",\"realm\"]",
        "\"clientIds\":[\"\"]",
        "\"outcomes\":[\"failed\"]",
        "\"subjects\":[\"events.>.login\"]",
        "\"subjects\":[\"events.*login\"]",
        "\"subjects\":[\"events..login\"]",
        "\"subjects\":[\"events.login\\n\"]"
      })
  void deliveryScopeValidationMatchesCaptureScopeValidation(String dimension) {
    assertRejected(
        document(
            rule(
                "rule", USER_MATCH.replace("}", "," + dimension + "}"), "{\"action\":\"retry\"}")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"kind\":\"user\",\"eventTypes\":[],\"realmIds\":[],\"subjects\":[]}",
        "{\"kind\":\"user\",\"eventTypes\":[\"*\"],\"subjects\":[\"*\",\"events.>\"]}",
        "{\"kind\":\"admin\",\"resourceType\":\"custom:widget\",\"operations\":[\"*\"]}",
        "{\"kind\":\"admin\",\"resourceType\":\"USER\","
            + "\"operations\":[\"CREATE\",\"UPDATE\"],\"userEnabled\":true}",
        "{\"kind\":\"admin\",\"resourceType\":\"custom:USER\","
            + "\"operations\":[\"UPDATE\"],\"userEnabled\":false}"
      })
  void existingScopeAndCustomResourceSemanticsArePreserved(String match) {
    assertAccepted(document(rule("rule", match, "{\"action\":\"retry\"}")));
  }
}
