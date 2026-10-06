package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.images.builder.Transferable;

/** Validates routed target identity against the packaged provider and actual admin endpoints. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class AdminUserAttributionIT extends IntegrationSupport {
  private static final String FILTER = "/opt/keycloak/conf/knd-attribution.json";
  private static final String ALL =
      """
      {"userEvents":["*"],"adminEvents":[{"resourceType":"*","operations":["*"]}]}
      """;
  private static final String DISABLED =
      """
      {"userEvents":[],"adminEvents":[
        {"resourceType":"USER","operations":["UPDATE"],"userEnabled":false}]}
      """;
  private static String realmId;

  @BeforeAll
  static void start() throws Exception {
    startInfrastructure(
        System.getProperty("keycloak.version", "26.7.4"),
        container ->
            container
                .withEnv("KND_FILTER_FILE", FILTER)
                .withEnv("KND_FILTER_RELOAD_MS", "100")
                .withCopyToContainer(Transferable.of(ALL), FILTER),
        false);
    realmId =
        objectMapper
            .readTree(request("GET", "/admin/realms/durable-test", null).body())
            .path("id")
            .asText();
    drained();
    broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
  }

  @AfterAll
  static void stop() throws Exception {
    stopInfrastructure();
  }

  @Test
  void trailingSlashUpdatesObserveStateAndRespectDisabledOnlyCapture() throws Exception {
    replace(ALL);
    String user = createUser();
    String path = "/admin/realms/durable-test/users/" + user;
    update(path, false);
    update(path + "/", true);
    update(path + "/", false);
    assertOrdered(user, List.of("CREATE", "UPDATE", "UPDATE", "UPDATE"));
    assertEquals(
        List.of(true, false, true, false),
        rows(user).stream().map(row -> row.at("/data/userEnabled").asBoolean()).toList());

    replace(DISABLED);
    update(path, true);
    update(path + "/", false);
    update(path + "/", true);
    update(path, false);
    assertOrdered(user, List.of("CREATE", "UPDATE", "UPDATE", "UPDATE", "UPDATE", "UPDATE"));
    var captured = rows(user);
    assertFalse(captured.get(4).at("/data/userEnabled").asBoolean());
    assertFalse(captured.get(5).at("/data/userEnabled").asBoolean());
    assertFalse(
        objectMapper.readTree(request("GET", path, null).body()).path("enabled").asBoolean());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "/logout/opaque+%2F/用户/"})
  void logoutSharesTheUsersSequenceAndPreservesOpaqueRoutedIds(String suffix) throws Exception {
    replace(ALL);
    String seed = UUID.randomUUID().toString();
    String user = (suffix.isEmpty() ? seed : seed.substring(0, 8)) + suffix;
    try (var connection = database.getConnection();
        var insert =
            connection.prepareStatement(
                "INSERT INTO user_entity"
                    + " (id,realm_id,username,enabled,email_verified,not_before,created_timestamp)"
                    + " VALUES (?,?,?,true,false,0,0)")) {
      insert.setString(1, user);
      insert.setString(2, realmId);
      insert.setString(3, "attribution-" + UUID.randomUUID());
      assertEquals(1, insert.executeUpdate());
    }
    String path =
        "/admin/realms/durable-test/users/"
            + URLEncoder.encode(user, StandardCharsets.UTF_8).replace("+", "%20");
    update(path, true);
    assertEquals(204, request("POST", path + "/logout", null).statusCode());
    update(path + "/", false);
    assertEquals(204, request("POST", path + "/logout/", null).statusCode());
    assertEquals(204, request("DELETE", path, null).statusCode());
    assertOrdered(user, List.of("UPDATE", "ACTION", "UPDATE", "ACTION", "DELETE"));
    var captured = rows(user);
    for (int index : new int[] {1, 3, 4}) {
      assertFalse(captured.get(index).path("data").has("userEnabled"));
    }
    assertEquals("users/" + user + "/logout", captured.get(1).at("/data/resourcePath").asText());
    assertEquals("users/" + user + "/logout/", captured.get(3).at("/data/resourcePath").asText());
  }

  private static void update(String path, boolean enabled) throws Exception {
    var response = request("PUT", path, Map.of("enabled", enabled));
    assertEquals(204, response.statusCode(), response.body());
  }

  private static void assertOrdered(String user, List<String> operations) throws Exception {
    var captured = rows(user);
    assertEquals(operations.size(), captured.size());
    String key = new EventOrdering(realmId, user, 1).key();
    for (int index = 0; index < captured.size(); index++) {
      var data = captured.get(index).path("data");
      assertEquals(user, data.path("userId").asText());
      assertNotEquals(user, data.path("actorUserId").asText());
      assertEquals(operations.get(index), data.path("operationType").asText());
      assertEquals(key, data.at("/ordering/key").asText());
      assertEquals(Integer.toString(index + 1), data.at("/ordering/sequence").asText());
    }
  }

  private static List<JsonNode> rows(String user) throws Exception {
    var events = new ArrayList<JsonNode>();
    try (var connection = database.getConnection();
        var query =
            connection.prepareStatement(
                "SELECT payload FROM kc_nats_outbox WHERE ordering_key=? ORDER BY user_sequence")) {
      query.setString(1, new EventOrdering(realmId, user, 1).key());
      try (var result = query.executeQuery()) {
        while (result.next()) {
          events.add(objectMapper.readTree(result.getString(1)));
        }
      }
    }
    return events;
  }

  private static void replace(String json) throws Exception {
    keycloak.copyFileToContainer(Transferable.of(json), FILTER + ".next");
    var rename = keycloak.execInContainer("mv", FILTER + ".next", FILTER);
    assertEquals(0, rename.getExitCode(), rename.getStderr());
    String digest = EventFilter.digest(json.getBytes(StandardCharsets.UTF_8));
    await()
        .atMost(Duration.ofSeconds(10))
        .until(
            () ->
                keycloak
                    .getLogs()
                    .lines()
                    .filter(line -> line.contains("Event filter applied; sha256="))
                    .reduce((previous, current) -> current)
                    .map(line -> line.contains("sha256=" + digest))
                    .orElse(false));
  }
}
