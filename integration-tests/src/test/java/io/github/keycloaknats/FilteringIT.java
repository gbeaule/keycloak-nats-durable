package io.github.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.nats.client.PullSubscribeOptions;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.images.builder.Transferable;

/** Exercises mounted policy replacement against real account transactions and pending delivery. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class FilteringIT extends IntegrationSupport {
  private static final String FILTER = "/opt/keycloak/conf/knd-events.json";
  private static final String ALL =
      """
      {"userEvents":["*"],"adminEvents":[{"resourceType":"*","operations":["*"]}]}
      """;

  @BeforeAll
  static void start() throws Exception {
    startInfrastructure(
        System.getProperty("keycloak.version"),
        container ->
            container
                .withEnv("KND_FILTER_FILE", FILTER)
                .withEnv("KND_FILTER_RELOAD_MS", "100")
                .withCopyToContainer(Transferable.of(ALL), FILTER));
  }

  @AfterAll
  static void stop() throws Exception {
    stopInfrastructure();
  }

  @Test
  void reloadFiltersBeforeCaptureWithoutDroppingPreviouslyCommittedEvents() throws Exception {
    drained();
    nats.jetStreamManagement().purgeStream(STREAM);
    var docker = broker.getDockerClient();
    docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    try {
      final String included = createUser();
      assertEquals(1, scalar("SELECT count(*) FROM kc_nats_outbox"));
      var originalIds = new HashSet<String>();
      try (var db = database.getConnection();
          var statement = db.createStatement();
          var rows = statement.executeQuery("SELECT id FROM kc_nats_outbox")) {
        while (rows.next()) {
          originalIds.add(rows.getString(1));
        }
      }
      replace(
          """
          {"userEvents":[],"adminEvents":[
            {"resourceType":"USER","operations":["UPDATE"],"userEnabled":false}]}
          """,
          true);
      final String excluded = createUser();
      assertEquals(1, scalar("SELECT count(*) FROM kc_nats_outbox"));
      assertEquals(
          204,
          request("PUT", "/admin/realms/durable-test/users/" + included, Map.of("enabled", false))
              .statusCode());
      assertEquals(2, scalar("SELECT count(*) FROM kc_nats_outbox"));
      assertEquals(
          204,
          request("PUT", "/admin/realms/durable-test/users/" + included, Map.of("enabled", true))
              .statusCode());
      assertEquals(2, scalar("SELECT count(*) FROM kc_nats_outbox"));

      replace("{\"userEvents\":[", false);
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> keycloak.getLogs().contains("Event filter reload rejected"));
      assertEquals(
          204,
          request("PUT", "/admin/realms/durable-test/users/" + excluded, Map.of("enabled", false))
              .statusCode());
      assertEquals(3, scalar("SELECT count(*) FROM kc_nats_outbox"));
      replace("{\"userEvents\":[],\"adminEvents\":[]}", true);
      createUser();
      assertEquals(3, scalar("SELECT count(*) FROM kc_nats_outbox"));

      docker.startContainerCmd(broker.getContainerId()).exec();
      connectNats();
      drained();
      var subscription =
          nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
      try {
        var messages = subscription.fetch(3, Duration.ofSeconds(10));
        assertEquals(3, messages.size());
        var delivered = new HashSet<String>();
        int disabled = 0;
        for (var message : messages) {
          var event = objectMapper.readTree(message.getData());
          delivered.add(event.path("id").asText());
          if (event.path("data").has("userEnabled")
              && !event.path("data").path("userEnabled").asBoolean()) {
            disabled++;
          }
          message.ackSync(Duration.ofSeconds(2));
        }
        assertTrue(delivered.containsAll(originalIds), "Reload must not filter persisted rows");
        assertEquals(2, disabled);
      } finally {
        subscription.unsubscribe();
      }
    } finally {
      if (!broker.isRunning()) {
        docker.startContainerCmd(broker.getContainerId()).exec();
      }
    }
  }

  private static void replace(String json, boolean valid) throws Exception {
    keycloak.copyFileToContainer(Transferable.of(json), FILTER + ".next");
    var rename = keycloak.execInContainer("mv", FILTER + ".next", FILTER);
    assertEquals(0, rename.getExitCode(), rename.getStderr());
    if (valid) {
      String digest =
          HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(json.getBytes(StandardCharsets.UTF_8)));
      await().atMost(Duration.ofSeconds(10)).until(() -> keycloak.getLogs().contains(digest));
    }
  }
}
