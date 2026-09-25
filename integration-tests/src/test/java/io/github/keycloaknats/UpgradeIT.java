package io.github.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.nats.client.PullSubscribeOptions;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Exercises an offline server replacement with an existing schema and unpublished account events.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class UpgradeIT extends IntegrationSupport {
  @BeforeAll
  static void start() throws Exception {
    startInfrastructure(System.getProperty("keycloak.upgrade.from", "26.6.4"));
  }

  @AfterAll
  static void stop() throws Exception {
    try {
      if (keycloak != null && keycloak.getContainerId() != null) {
        Files.writeString(Path.of("target/keycloak-upgrade-target.log"), keycloak.getLogs());
      }
    } finally {
      stopInfrastructure();
    }
  }

  @Test
  void pendingEventsSurviveServerReplacementWithTheSameDatabase() throws Exception {
    drained();
    nats.jetStreamManagement().purgeStream(STREAM);
    var docker = broker.getDockerClient();
    docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    String user = createUser();
    assertEquals(
        204,
        request("PUT", "/admin/realms/durable-test/users/" + user, Map.of("enabled", false))
            .statusCode());
    assertEquals(
        204, request("DELETE", "/admin/realms/durable-test/users/" + user, null).statusCode());
    Map<String, StoredEvent> pending = pendingEvents();
    assertEquals(3, pending.size());

    Files.writeString(Path.of("target/keycloak-upgrade-source.log"), keycloak.getLogs());
    keycloak.stop();
    keycloak = keycloakContainer(false);
    keycloak.start();
    loginAdmin();
    assertEquals(
        404, request("GET", "/admin/realms/durable-test/users/" + user, null).statusCode());
    assertEquals(
        pending, pendingEvents(), "Migration must preserve persisted IDs, subjects and bytes");

    docker.startContainerCmd(broker.getContainerId()).exec();
    connectNats();
    drained();
    var subscription = nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
    try {
      var delivered = new HashMap<String, StoredEvent>();
      for (var message : subscription.fetch(3, Duration.ofSeconds(10))) {
        String payload = new String(message.getData(), StandardCharsets.UTF_8);
        String id = objectMapper.readTree(payload).path("id").asText();
        assertEquals(id, message.getHeaders().getFirst("Nats-Msg-Id"));
        delivered.put(id, new StoredEvent(message.getSubject(), payload));
        message.ackSync(Duration.ofSeconds(2));
      }
      assertEquals(pending, delivered);
      assertEquals(0, messages());
      createUser();
      drained();
      var fresh = subscription.fetch(1, Duration.ofSeconds(5));
      assertEquals(1, fresh.size(), "The new server must still capture events after schema reuse");
      assertTrue(
          objectMapper
              .readTree(fresh.getFirst().getData())
              .path("data")
              .path("userId")
              .isTextual());
      fresh.getFirst().ackSync(Duration.ofSeconds(2));
    } finally {
      subscription.unsubscribe();
    }
  }

  private record StoredEvent(String subject, String payload) {}

  private static Map<String, StoredEvent> pendingEvents() throws Exception {
    var pending = new HashMap<String, StoredEvent>();
    try (var connection = database.getConnection();
        var statement = connection.createStatement();
        var rows = statement.executeQuery("SELECT id, subject, payload FROM kc_nats_outbox")) {
      while (rows.next()) {
        pending.put(rows.getString(1), new StoredEvent(rows.getString(2), rows.getString(3)));
      }
    }
    return pending;
  }
}
