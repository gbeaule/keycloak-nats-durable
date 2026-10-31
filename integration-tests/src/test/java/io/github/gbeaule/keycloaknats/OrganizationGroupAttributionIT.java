package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.images.builder.Transferable;

/**
 * Real organization group endpoints must join the affected user's existing publication sequence.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OrganizationGroupAttributionIT extends IntegrationSupport {
  private static final String FILTER =
      """
      {"userEvents":[],"adminEvents":[
        {"resourceType":"USER","operations":["*"]},
        {"resourceType":"ORGANIZATION_GROUP_MEMBERSHIP","operations":["CREATE","DELETE"]}]}
      """;
  private static String realmId;

  @BeforeAll
  static void start() throws Exception {
    startInfrastructure(
        System.getProperty("keycloak.version", "26.8.1"),
        container ->
            container
                .withEnv("KND_FILTER_FILE", "/opt/keycloak/conf/knd-organization-groups.json")
                .withCopyToContainer(
                    Transferable.of(FILTER), "/opt/keycloak/conf/knd-organization-groups.json"),
        false);
    var realm = request("GET", "/admin/realms/durable-test", null);
    assertEquals(200, realm.statusCode(), realm.body());
    realmId = objectMapper.readTree(realm.body()).path("id").asText();
    assertEquals(
        204,
        request("PUT", "/admin/realms/durable-test", Map.of("organizationsEnabled", true))
            .statusCode());
    drained();
    broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
  }

  @AfterAll
  static void stop() throws Exception {
    stopInfrastructure();
  }

  @ParameterizedTest
  @CsvSource({"false,''", "true,/"})
  void joinAndLeavePreserveTheTargetIdentityAndCannotBypassItsRetryingPredecessor(
      boolean opaque, String suffix) throws Exception {
    String name = "organization-" + UUID.randomUUID();
    var created =
        request(
            "POST",
            "/admin/realms/durable-test/organizations",
            Map.of("name", name, "alias", name));
    assertEquals(201, created.statusCode(), created.body());
    String location = created.headers().firstValue("Location").orElseThrow();
    String organization = location.substring(location.lastIndexOf('/') + 1);
    assertFalse(organization.isBlank());
    String organizationPath = "/admin/realms/durable-test/organizations/" + organization;
    var groupCreated = request("POST", organizationPath + "/groups", Map.of("name", "staff"));
    assertEquals(201, groupCreated.statusCode(), groupCreated.body());
    String group = objectMapper.readTree(groupCreated.body()).path("id").asText();
    assertFalse(group.isBlank());
    String user = opaque ? createOpaqueUser() : createUser();
    String encodedUser = URLEncoder.encode(user, StandardCharsets.UTF_8).replace("+", "%20");
    var added = request("POST", organizationPath + "/members", user);
    assertEquals(201, added.statusCode(), added.body());
    final String resource =
        "organizations/" + organization + "/groups/" + group + "/members/" + user;
    String endpoint = organizationPath + "/groups/" + group + "/members/" + encodedUser + suffix;
    var joined = request("PUT", endpoint, null);
    assertEquals(204, joined.statusCode(), joined.body());
    assertEquals(List.of(user), memberIds(organizationPath + "/groups/" + group + "/members"));
    var left = request("DELETE", endpoint, null);
    assertEquals(204, left.statusCode(), left.body());
    assertTrue(memberIds(organizationPath + "/groups/" + group + "/members").isEmpty());
    var captured = rows(user);
    assertEquals(3, captured.size(), "Both group operations must join the target user's sequence");
    String key = new EventOrdering(realmId, user, 1).key();
    for (int index = 0; index < captured.size(); index++) {
      var data = captured.get(index).path("data");
      assertEquals(user, data.path("userId").asText());
      assertNotEquals(user, data.path("actorUserId").asText());
      assertFalse(data.path("actorUserId").asText().isBlank());
      assertEquals(key, data.at("/ordering/key").asText());
      assertEquals(Integer.toString(index + 1), data.at("/ordering/sequence").asText());
    }
    assertEquals(
        opaque ? "UPDATE" : "CREATE", captured.getFirst().at("/data/operationType").asText());
    for (int index = 1; index <= 2; index++) {
      var data = captured.get(index).path("data");
      assertEquals("ORGANIZATION_GROUP_MEMBERSHIP", data.path("resourceType").asText());
      assertEquals(index == 1 ? "CREATE" : "DELETE", data.path("operationType").asText());
      assertEquals(resource + suffix, data.path("resourcePath").asText());
      assertFalse(data.has("userEnabled"));
    }
    String predecessor = captured.getFirst().path("id").asText();
    await().atMost(Duration.ofSeconds(10)).until(() -> attempts(predecessor) > 0);
    // Delay the actual failed predecessor while making the broker available for other users.
    reschedule(predecessor, Long.MAX_VALUE);
    var docker = broker.getDockerClient();
    docker.startContainerCmd(broker.getContainerId()).exec();
    try {
      connectNats();
      String unrelated = createUser();
      await().atMost(Duration.ofSeconds(30)).until(() -> published(unrelated).size() == 1);
      await()
          .during(Duration.ofSeconds(2))
          .atMost(Duration.ofSeconds(5))
          .until(() -> rows(user).size() == 3 && published(user).isEmpty());
      reschedule(predecessor, 0);
      drained();
      assertEquals(
          captured.stream().map(row -> row.path("id").asText()).toList(),
          published(user).stream().map(row -> row.path("id").asText()).toList());
    } finally {
      docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    }
  }

  private static String createOpaqueUser() throws Exception {
    String user = UUID.randomUUID().toString().substring(0, 8) + "/members/+%2F/用户/";
    try (var db = database.getConnection();
        var insert =
            db.prepareStatement(
                "INSERT INTO user_entity"
                    + " (id,realm_id,username,enabled,email_verified,not_before,created_timestamp)"
                    + " VALUES (?,?,?,true,false,0,0)")) {
      insert.setString(1, user);
      insert.setString(2, realmId);
      insert.setString(3, "organization-member-" + UUID.randomUUID());
      assertEquals(1, insert.executeUpdate());
    }
    var updated =
        request(
            "PUT",
            "/admin/realms/durable-test/users/"
                + URLEncoder.encode(user, StandardCharsets.UTF_8).replace("+", "%20"),
            Map.of("enabled", true));
    assertEquals(204, updated.statusCode(), updated.body());
    return user;
  }

  private static List<String> memberIds(String path) throws Exception {
    var response = request("GET", path, null);
    assertEquals(200, response.statusCode(), response.body());
    var ids = new ArrayList<String>();
    for (var member : objectMapper.readTree(response.body())) {
      ids.add(member.path("id").asText());
    }
    return ids;
  }

  private static long attempts(String id) throws Exception {
    try (var db = database.getConnection();
        var query = db.prepareStatement("SELECT attempts FROM kc_nats_outbox WHERE id=?")) {
      query.setString(1, id);
      try (var result = query.executeQuery()) {
        assertTrue(result.next());
        return result.getLong(1);
      }
    }
  }

  private static void reschedule(String id, long nextAttempt) throws Exception {
    try (var db = database.getConnection();
        var update = db.prepareStatement("UPDATE kc_nats_outbox SET next_attempt_at=? WHERE id=?");
        var head =
            db.prepareStatement(
                "UPDATE kc_nats_capture_counter SET head_next_attempt_at=?"
                    + " WHERE head_event_id=?")) {
      db.setAutoCommit(false);
      // Match publication resolution: own the event before updating its indexed head projection.
      update.setQueryTimeout(5);
      update.setLong(1, nextAttempt);
      update.setString(2, id);
      assertEquals(1, update.executeUpdate());
      head.setQueryTimeout(5);
      head.setLong(1, nextAttempt);
      head.setString(2, id);
      assertEquals(1, head.executeUpdate());
      db.commit();
    }
  }

  private static List<JsonNode> rows(String user) throws Exception {
    var events = new ArrayList<JsonNode>();
    String key = new EventOrdering(realmId, user, 1).key();
    try (var db = database.getConnection();
        var query =
            db.prepareStatement(
                "SELECT payload,ordering_key,user_sequence FROM kc_nats_outbox"
                    + " WHERE ordering_key=? ORDER BY user_sequence")) {
      query.setString(1, key);
      try (var result = query.executeQuery()) {
        while (result.next()) {
          var event = objectMapper.readTree(result.getString(1));
          assertEquals(key, result.getString(2));
          assertEquals(
              Long.toString(result.getLong(3)), event.at("/data/ordering/sequence").asText());
          events.add(event);
        }
      }
    }
    return events;
  }

  private static List<JsonNode> published(String user) throws Exception {
    var events = new ArrayList<JsonNode>();
    long lastSequence =
        nats.jetStreamManagement().getStreamInfo(STREAM).getStreamState().getLastSequence();
    for (long sequence = 1; sequence <= lastSequence; sequence++) {
      var event =
          objectMapper.readTree(nats.jetStreamManagement().getMessage(STREAM, sequence).getData());
      if (user.equals(event.at("/data/userId").asText())) {
        events.add(event);
      }
    }
    return events;
  }
}
