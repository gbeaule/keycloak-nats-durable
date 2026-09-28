package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/** Two installed providers sharing user positions through a source database crash. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class PerUserSystemIT extends IntegrationSupport {
  private static final String SCHEMA = "\"ordering-system\".";
  private static final String OUTBOX = SCHEMA + "kc_nats_outbox";
  private static final String COUNTERS = SCHEMA + "kc_nats_capture_counter";
  private static final String LOGIN =
      "grant_type=password&client_id=test-client&username=alice&password=alice-password";

  @BeforeAll
  static void start() throws Exception {
    startInfrastructure(
        System.getProperty("keycloak.version"),
        node -> {
          try {
            execute("CREATE SCHEMA \"ordering-system\"");
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
          configure(node);
        },
        false);
  }

  @AfterAll
  static void stop() throws Exception {
    stopInfrastructure();
  }

  @Test
  void nodesShareCaptureLocksAndRecoverOriginalsWithoutAReceiver() throws Exception {
    var second = configure(keycloakContainer(false));
    try {
      second.start();
      loginAdmin();
      broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
      var lookup = request("GET", "/admin/realms/durable-test/users?username=alice", null);
      assertEquals(200, lookup.statusCode(), lookup.body());
      var users = objectMapper.readTree(lookup.body());
      assertEquals(1, users.size(), lookup.body());
      String user = users.get(0).path("id").asText();
      assertEquals(200, login("durable-test", LOGIN).statusCode());

      try (var executor = Executors.newVirtualThreadPerTaskExecutor();
          var owner = database.getConnection()) {
        owner.setAutoCommit(false);
        try (var lock =
            owner.prepareStatement(
                "SELECT last_sequence FROM " + COUNTERS + " WHERE user_id=? FOR UPDATE")) {
          lock.setString(1, user);
          try (var rows = lock.executeQuery()) {
            assertTrue(rows.next());
            assertEquals(1, rows.getLong(1));
          }
        }
        var firstLogin = executor.submit(() -> loginTo(keycloak, "durable-test", LOGIN));
        var secondLogin = executor.submit(() -> loginTo(second, "durable-test", LOGIN));
        try {
          await()
              .atMost(Duration.ofSeconds(8))
              .until(
                  () ->
                      scalar(
                              "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock'"
                                  + " AND query ILIKE '%kc_nats_capture_counter%'")
                          >= 2);
          assertFalse(firstLogin.isDone());
          assertFalse(secondLogin.isDone());
          createUserOn(second);
          assertEquals(
              1, scalar("SELECT last_sequence FROM " + COUNTERS + " WHERE user_id='" + user + "'"));
          assertEquals(2, scalar("SELECT count(*) FROM " + OUTBOX));
        } finally {
          owner.rollback();
        }
        assertEquals(200, firstLogin.get(10, TimeUnit.SECONDS).statusCode());
        assertEquals(200, secondLogin.get(10, TimeUnit.SECONDS).statusCode());
      }

      // A failed account transaction must not consume a user position on either node.
      execute(
          "ALTER TABLE "
              + OUTBOX
              + " ADD CONSTRAINT reject_update CHECK"
              + " ((payload::jsonb #>> '{data,operationType}')"
              + " IS DISTINCT FROM 'UPDATE') NOT VALID");
      try {
        assertTrue(
            requestTo(
                        second,
                        "PUT",
                        "/admin/realms/durable-test/users/" + user,
                        Map.of("enabled", false))
                    .statusCode()
                >= 400);
        assertEquals(
            3, scalar("SELECT last_sequence FROM " + COUNTERS + " WHERE user_id='" + user + "'"));
        assertEquals(
            1,
            scalar(
                "SELECT count(*) FROM "
                    + SCHEMA
                    + "user_entity WHERE id='"
                    + user
                    + "' AND enabled=true"));
      } finally {
        execute("ALTER TABLE " + OUTBOX + " DROP CONSTRAINT reject_update");
      }

      try (var executor = Executors.newFixedThreadPool(4)) {
        var requests = new ArrayList<java.util.concurrent.Future<?>>();
        for (int i = 0; i < 8; i++) {
          var node = i % 2 == 0 ? keycloak : second;
          boolean authentication = i % 4 < 2;
          requests.add(
              executor.submit(
                  () -> {
                    if (authentication) {
                      assertEquals(200, loginTo(node, "durable-test", LOGIN).statusCode());
                    } else {
                      assertEquals(
                          204,
                          requestTo(
                                  node,
                                  "PUT",
                                  "/admin/realms/durable-test/users/" + user,
                                  Map.of("firstName", "Updated"))
                              .statusCode());
                    }
                    return null;
                  }));
        }
        for (var request : requests) {
          request.get(20, TimeUnit.SECONDS);
        }
      }
      assertEquals(
          204,
          requestTo(
                  second,
                  "PUT",
                  "/admin/realms/durable-test/users/" + user + "/reset-password",
                  Map.of("type", "password", "value", "alice-password", "temporary", false))
              .statusCode());
      var originals = pending();
      assertEquals(13, originals.size());
      assertEquals(
          12, scalar("SELECT last_sequence FROM " + COUNTERS + " WHERE user_id='" + user + "'"));
      second.stop();

      var docker = postgres.getDockerClient();
      docker.killContainerCmd(postgres.getContainerId()).withSignal("KILL").exec();
      docker.startContainerCmd(postgres.getContainerId()).exec();
      database.setURL(
          "jdbc:postgresql://"
              + postgres.getHost()
              + ":"
              + currentPort(postgres, 5432)
              + "/keycloak");
      await()
          .atMost(Duration.ofSeconds(30))
          .ignoreExceptions()
          .untilAsserted(() -> assertEquals(originals, pending()));
      restartKeycloak();
      assertEquals(originals, pending());
      broker.getDockerClient().startContainerCmd(broker.getContainerId()).exec();
      connectNats();
      await()
          .atMost(Duration.ofSeconds(30))
          .until(() -> scalar("SELECT count(*) FROM " + OUTBOX) == 0);

      assertEquals(originals.size(), messages());
      var received = new LinkedHashMap<String, Original>();
      long last = 0;
      boolean nested = false;
      var state = nats.jetStreamManagement().getStreamInfo(STREAM).getStreamState();
      for (long sequence = state.getFirstSequence();
          sequence <= state.getLastSequence();
          sequence++) {
        var message = nats.jetStreamManagement().getMessage(STREAM, sequence);
        var event = objectMapper.readTree(message.getData());
        String id = event.path("id").asText();
        assertEquals(id, message.getHeaders().getFirst("Nats-Msg-Id"));
        received.put(
            id,
            new Original(
                message.getSubject(), new String(message.getData(), StandardCharsets.UTF_8)));
        var data = event.path("data");
        if (user.equals(data.path("userId").asText())) {
          assertEquals(++last, data.path("ordering").path("sequence").asLong());
          if (data.has("actorUserId")) {
            assertNotEquals(user, data.path("actorUserId").asText());
          }
          nested |= data.path("resourcePath").asText().endsWith("/reset-password");
        }
      }
      assertEquals(12, last);
      assertTrue(nested);
      assertEquals(originals, received, "Every published record must be a captured original");
      assertEquals(2, scalar("SELECT count(*) FROM " + COUNTERS));
      assertEquals(0, scalar("SELECT count(*) FROM " + SCHEMA + "kc_nats_discard_audit"));
      assertTrue(nats.jetStreamManagement().getConsumerNames(STREAM).isEmpty());
    } finally {
      if (second.getContainerId() != null) {
        Files.writeString(Path.of("target/ordering-second-node.log"), second.getLogs());
      }
      second.stop();
    }
  }

  private static GenericContainer<?> configure(GenericContainer<?> node) {
    return node.withEnv("KC_DB_SCHEMA", "ordering-system")
        .withEnv("KC_HOSTNAME", "http://keycloak.test")
        .withEnv("KND_RELAY_WORKERS", "2");
  }

  private record Original(String subject, String payload) {}

  private static Map<String, Original> pending() throws Exception {
    var result = new LinkedHashMap<String, Original>();
    try (var connection = database.getConnection();
        var query = connection.createStatement();
        var rows = query.executeQuery("SELECT id, subject, payload FROM " + OUTBOX)) {
      while (rows.next()) {
        result.put(rows.getString(1), new Original(rows.getString(2), rows.getString(3)));
      }
    }
    return result;
  }
}
