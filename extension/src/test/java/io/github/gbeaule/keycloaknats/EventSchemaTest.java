package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;

class EventSchemaTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Schema eventSchema =
      SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
          .getSchema(EventSchemaTest.class.getResourceAsStream("/schemas/event-v1.schema.json"));

  static void assertValid(JsonNode event) {
    var errors = validate(event);
    assertTrue(errors.isEmpty(), errors::toString);
  }

  private static List<Error> validate(JsonNode event) {
    // Keycloak uses Jackson 2; the validator uses Jackson 3. Exchange serialized JSON.
    return eventSchema.validate(event.toString(), InputFormat.JSON);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "user-login",
        "user-login-error",
        "user-minimal",
        "admin-create",
        "admin-disable",
        "admin-enable",
        "admin-delete",
        "admin-action",
        "admin-error",
        "admin-nested-resource",
        "admin-custom-resource",
        "admin-minimal"
      })
  void documentationExamplesConformToSchema(String example) throws IOException {
    try (var input = getClass().getResourceAsStream("/examples/" + example + ".json")) {
      assertNotNull(input, "Missing documentation example: " + example);
      assertValid(objectMapper.readTree(input));
    }
  }

  @Test
  void schemaRejectsMixedFamiliesAndInvalidStateClaims() throws IOException {
    ObjectNode event =
        (ObjectNode)
            objectMapper.readTree(getClass().getResourceAsStream("/examples/user-login.json"));
    ((ObjectNode) event.get("data")).put("operationType", "UPDATE");
    assertFalse(validate(event).isEmpty());
    event =
        (ObjectNode)
            objectMapper.readTree(getClass().getResourceAsStream("/examples/admin-delete.json"));
    ((ObjectNode) event.get("data")).put("userEnabled", false);
    assertFalse(validate(event).isEmpty());
  }

  @Test
  void sessionIdIsAnOptionalStringOnUserEventsOnly() throws IOException {
    var event = objectMapper.readTree(getClass().getResourceAsStream("/examples/user-login.json"));
    var data = (ObjectNode) event.get("data");
    data.putNull("sessionId");
    assertFalse(validate(event).isEmpty());
    data.put("sessionId", 123);
    assertFalse(validate(event).isEmpty());
    data.remove("sessionId");
    assertValid(event);
    data.put("sessionId", "s".repeat(255));
    assertValid(event);
    data.put("sessionId", "s".repeat(256));
    assertFalse(validate(event).isEmpty());
    event = objectMapper.readTree(getClass().getResourceAsStream("/examples/admin-delete.json"));
    ((ObjectNode) event.get("data")).put("sessionId", "actor-session");
    assertFalse(validate(event).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0",
        "-1",
        "+1",
        "01",
        "1.0",
        "1e3",
        "1\n",
        "١",
        "",
        "9223372036854775808",
        "10000000000000000000"
      })
  void orderingSequenceRejectsMalformedAndOverflowingStrings(String sequence) throws IOException {
    var event = objectMapper.readTree(getClass().getResourceAsStream("/examples/user-login.json"));
    ((ObjectNode) event.at("/data/ordering")).put("sequence", sequence);
    assertFalse(validate(event).isEmpty(), sequence);
  }

  @Test
  void orderingRequiresUserIdentityAndOnlyTheTwoContractFields() throws IOException {
    var event = objectMapper.readTree(getClass().getResourceAsStream("/examples/user-login.json"));
    var data = (ObjectNode) event.get("data");
    var ordering = (ObjectNode) data.get("ordering");
    ordering.put("sequence", 1);
    assertFalse(validate(event).isEmpty());
    ordering.put("sequence", "9223372036854775807");
    assertValid(event);
    ordering.put("maxFailures", 1);
    assertFalse(validate(event).isEmpty());
    ordering.remove("maxFailures");
    ordering.put("key", "u.realm.with.dots.user");
    assertFalse(validate(event).isEmpty());
    ordering.put("key", "u.ZGVtbw.dXNlci0xMjM");
    data.remove("userId");
    assertFalse(validate(event).isEmpty());
    data.remove("ordering");
    assertValid(event);
  }

  @Test
  void publishedCatalogueContainsEveryEnumInCompileBaseline() throws IOException {
    var catalogue =
        objectMapper.readTree(
            getClass().getResourceAsStream("/schemas/keycloak-catalogue-26.7.4.json"));
    assertEquals(enumNames(EventType.values()), names(catalogue.get("userEventTypes")));
    assertEquals(enumNames(ResourceType.values()), names(catalogue.get("adminResourceTypes")));
    assertEquals(enumNames(OperationType.values()), names(catalogue.get("adminOperationTypes")));
  }

  private static Set<String> enumNames(Enum<?>[] values) {
    return Arrays.stream(values).map(Enum::name).collect(Collectors.toSet());
  }

  private static Set<String> names(JsonNode values) {
    Set<String> names = new HashSet<>();
    values.forEach(value -> names.add(value.asText()));
    return names;
  }
}
