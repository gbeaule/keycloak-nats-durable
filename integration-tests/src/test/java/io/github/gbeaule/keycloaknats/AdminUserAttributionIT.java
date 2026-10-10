package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        System.getProperty("keycloak.version", "26.7.5"),
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
  @ValueSource(strings = {"", "/"})
  void bruteForceResetSharesTheTargetSequenceAndCannotBypassItsPendingPredecessor(String suffix)
      throws Exception {
    replace(ALL);
    assertEquals(
        204,
        request("PUT", "/admin/realms/durable-test", Map.of("bruteForceProtected", true))
            .statusCode());
    String username = "brute-force-" + UUID.randomUUID();
    var created =
        request(
            "POST",
            "/admin/realms/durable-test/users",
            Map.of(
                "username",
                username,
                "enabled",
                true,
                "credentials",
                List.of(
                    Map.of("type", "password", "value", "correct-password", "temporary", false))));
    assertEquals(201, created.statusCode(), created.body());
    String location = created.headers().firstValue("Location").orElseThrow();
    String user = location.substring(location.lastIndexOf('/') + 1);
    assertEquals(
        400,
        login(
                "durable-test",
                "grant_type=password&client_id=test-client&username="
                    + username
                    + "&password=incorrect")
            .statusCode());
    String endpoint = "/admin/realms/durable-test/attack-detection/brute-force/users/" + user;
    await()
        .atMost(Duration.ofSeconds(10))
        .until(
            () ->
                objectMapper
                        .readTree(request("GET", endpoint, null).body())
                        .path("numFailures")
                        .asInt()
                    > 0);
    assertEquals(204, request("DELETE", endpoint + suffix, null).statusCode());
    var captured = rows(user);
    assertEquals(3, captured.size());
    assertUserOrdering(user, captured);
    assertEquals("CREATE", captured.getFirst().at("/data/operationType").asText());
    assertEquals("LOGIN_ERROR", captured.get(1).at("/data/eventType").asText());
    var reset = captured.getLast().path("data");
    assertEquals("USER_LOGIN_FAILURE", reset.path("resourceType").asText());
    assertEquals("DELETE", reset.path("operationType").asText());
    assertEquals(
        "attack-detection/brute-force/users/" + user + suffix, reset.path("resourcePath").asText());
    assertFalse(reset.has("userEnabled"));

    // Hold only this user's predecessor; an unrelated publication proves the relay is active.
    try (var db = database.getConnection();
        var lock = db.prepareStatement("SELECT id FROM kc_nats_outbox WHERE id=? FOR UPDATE")) {
      db.setAutoCommit(false);
      lock.setQueryTimeout(5);
      lock.setString(1, captured.getFirst().path("id").asText());
      try (var result = lock.executeQuery()) {
        assertTrue(result.next());
      }
      var docker = broker.getDockerClient();
      docker.startContainerCmd(broker.getContainerId()).exec();
      try {
        connectNats();
        String unrelated = createUser();
        // A stopped container's missing DNS alias can remain negatively cached during recovery.
        await().atMost(Duration.ofSeconds(30)).until(() -> published(unrelated).size() == 1);
        await()
            .during(Duration.ofSeconds(2))
            .atMost(Duration.ofSeconds(5))
            .until(() -> rows(user).size() == captured.size());
        assertTrue(published(user).isEmpty(), "The reset must not overtake the locked predecessor");
        db.commit();
        drained();
        assertEquals(
            captured.stream().map(row -> row.path("id").asText()).toList(),
            published(user).stream().map(row -> row.path("id").asText()).toList());
      } finally {
        docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
      }
    }
  }

  @Test
  void bulkBruteForceResetDoesNotAssignTheActorAsAnAffectedUser() throws Exception {
    replace(ALL);
    String resource = "attack-detection/brute-force/users";
    assertEquals(
        204, request("DELETE", "/admin/realms/durable-test/" + resource, null).statusCode());
    try (var db = database.getConnection();
        var query =
            db.prepareStatement(
                "SELECT payload,ordering_key,user_sequence FROM kc_nats_outbox"
                    + " WHERE payload::jsonb->'data'->>'resourcePath'=?")) {
      query.setString(1, resource);
      try (var result = query.executeQuery()) {
        assertTrue(result.next());
        var data = objectMapper.readTree(result.getString(1)).path("data");
        assertEquals("USER_LOGIN_FAILURE", data.path("resourceType").asText());
        assertFalse(data.has("userId"));
        assertFalse(data.has("ordering"));
        assertFalse(data.path("actorUserId").asText().isBlank());
        assertNull(result.getString(2));
        assertNull(result.getObject(3));
        assertFalse(result.next());
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({"false,''", "true,/"})
  void organizationMemberDeletionSharesTheUserSequenceForManagedAndUnmanagedMembers(
      boolean managed, String suffix) throws Exception {
    replace(ALL);
    assertEquals(
        204,
        request("PUT", "/admin/realms/durable-test", Map.of("organizationsEnabled", true))
            .statusCode());
    String name = "organization-" + UUID.randomUUID();
    var created =
        request(
            "POST",
            "/admin/realms/durable-test/organizations",
            Map.of("name", name, "alias", name));
    assertEquals(201, created.statusCode(), created.body());
    String location = created.headers().firstValue("Location").orElseThrow();
    String organization = location.substring(location.lastIndexOf('/') + 1);
    String user = createUser();
    String members = "organizations/" + organization + "/members";
    var added = request("POST", "/admin/realms/durable-test/" + members, user);
    assertEquals(201, added.statusCode(), added.body());
    assertEquals(1, rows(user).size());
    // Collection-level creation has no routed member ID; it must remain independent.
    try (var db = database.getConnection();
        var query =
            db.prepareStatement(
                "SELECT payload,ordering_key FROM kc_nats_outbox"
                    + " WHERE payload::jsonb->'data'->>'resourcePath'=?")) {
      query.setString(1, members);
      try (var result = query.executeQuery()) {
        assertTrue(result.next());
        assertFalse(objectMapper.readTree(result.getString(1)).path("data").has("userId"));
        assertNull(result.getString(2));
        assertFalse(result.next());
      }
    }
    if (managed) {
      // Set up the membership produced by organization identity-provider registration.
      try (var db = database.getConnection();
          var update =
              db.prepareStatement(
                  "UPDATE user_group_membership SET membership_type='MANAGED'"
                      + " WHERE user_id=?"
                      + " AND group_id=(SELECT group_id FROM org WHERE id=?)")) {
        update.setString(1, user);
        update.setString(2, organization);
        assertEquals(1, update.executeUpdate());
      }
    }
    String member = "/admin/realms/durable-test/" + members + "/" + user;
    var observed = request("GET", member, null);
    assertEquals(200, observed.statusCode(), observed.body());
    assertEquals(
        managed ? "MANAGED" : "UNMANAGED",
        objectMapper.readTree(observed.body()).path("membershipType").asText());
    replace(
        """
        {"userEvents":[],"adminEvents":[
          {"resourceType":"ORGANIZATION_MEMBERSHIP","operations":["DELETE"]}]}
        """);
    var deleted = request("DELETE", member + suffix, null);
    assertEquals(204, deleted.statusCode(), deleted.body());
    var captured = rows(user);
    assertEquals(2, captured.size());
    assertUserOrdering(user, captured);
    var data = captured.getLast().path("data");
    assertEquals("ORGANIZATION_MEMBERSHIP", data.path("resourceType").asText());
    assertEquals("DELETE", data.path("operationType").asText());
    assertEquals(members + "/" + user + suffix, data.path("resourcePath").asText());
    assertFalse(data.has("userEnabled"));
    assertEquals(
        managed ? 404 : 200,
        request("GET", "/admin/realms/durable-test/users/" + user, null).statusCode());
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
    assertUserOrdering(user, captured);
    for (int index = 0; index < captured.size(); index++) {
      assertEquals(operations.get(index), captured.get(index).at("/data/operationType").asText());
    }
  }

  private static void assertUserOrdering(String user, List<JsonNode> captured) {
    String key = new EventOrdering(realmId, user, 1).key();
    for (int index = 0; index < captured.size(); index++) {
      var data = captured.get(index).path("data");
      assertEquals(user, data.path("userId").asText());
      assertNotEquals(user, data.path("actorUserId").asText());
      assertEquals(key, data.at("/ordering/key").asText());
      assertEquals(Integer.toString(index + 1), data.at("/ordering/sequence").asText());
    }
  }

  private static List<JsonNode> published(String user) throws Exception {
    var events = new ArrayList<JsonNode>();
    long count = messages();
    for (long sequence = 1; sequence <= count; sequence++) {
      var event =
          objectMapper.readTree(nats.jetStreamManagement().getMessage(STREAM, sequence).getData());
      if (user.equals(event.at("/data/userId").asText())) {
        events.add(event);
      }
    }
    return events;
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
