package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.gbeaule.keycloaknats.consumer.OutboxReport;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.jar.JarFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.images.builder.Transferable;

/** Packaged producer, local diagnostics and restart without any receiver or consumer database. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class PerUserPublicationIT extends IntegrationSupport {
  @BeforeAll
  static void start() throws Exception {
    String policy =
        """
        {"userEvents":["LOGIN"],"adminEvents":[{"resourceType":"USER","operations":["*"]}],
         "delivery":{"rules":[{"id":"ephemeral-login","match":{"kind":"user","eventTypes":["LOGIN"]},
           "policy":{"action":"discard","maxAgeSeconds":3,"maxFailures":1}}]}}
        """;
    startInfrastructure(
        System.getProperty("keycloak.version", "26.7.4"),
        node ->
            node.withExposedPorts(8080, 9000)
                .withEnv("KC_METRICS_ENABLED", "true")
                .withEnv("KND_RELAY_WORKERS", "2")
                .withEnv("KND_AUDIT_CLEANUP_INTERVAL_MS", "1000")
                .withEnv("KND_FILTER_FILE", "/opt/keycloak/conf/events.json")
                .withCopyToContainer(Transferable.of(policy), "/opt/keycloak/conf/events.json"),
        false);
  }

  @AfterAll
  static void stop() throws Exception {
    stopInfrastructure();
  }

  @Test
  void outageDiscardCleanupAndRestartNeedOnlyTheProducerStores() throws Exception {
    assertTrue(nats.jetStreamManagement().getConsumerNames(STREAM).isEmpty());
    assertEquals(0, scalar("SELECT count(*) FROM pg_database WHERE datname='consumer'"));
    try (var jar = new JarFile(System.getProperty("extension.jar"))) {
      assertFalse(jar.stream().anyMatch(e -> e.getName().contains("/consumer/")));
    }
    execute("CREATE ROLE knd_monitor LOGIN PASSWORD 'monitor-test'");
    execute(Files.readString(Path.of("../deploy/outbox-monitor.sql")));
    var reader = new PGSimpleDataSource();
    reader.setURL(postgres.getJdbcUrl());
    reader.setUser("knd_monitor");
    reader.setPassword("monitor-test");

    var docker = broker.getDockerClient();
    docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    String user;
    try {
      user = createUser();
      assertEquals(
          204,
          request("PUT", "/admin/realms/durable-test/users/" + user, Map.of("enabled", false))
              .statusCode());
      assertEquals(
          200,
          login(
                  "durable-test",
                  "grant_type=password&client_id=test-client"
                      + "&username=alice&password=alice-password")
              .statusCode());
      await()
          .atMost(Duration.ofSeconds(15))
          .until(() -> scalar("SELECT count(*) FROM kc_nats_discard_audit") == 1);
      var report = OutboxReport.collect(reader, "public", ">", "", 5);
      assertEquals(2L, report.get("pending"));
      assertEquals(1L, report.get("blocked_users"));
      assertEquals(1L, report.get("blocked_events"));
      assertEquals(1L, report.get("retrying_heads"));
      assertEquals(1L, report.get("audit_retained"));
      var updates =
          OutboxReport.collect(reader, "public", "keycloak.events.*.admin.user.update", "", 5);
      assertEquals(1L, updates.get("blocked_users"));
      assertEquals(0L, updates.get("due"), "Filtering must not hide a predecessor");
      assertEquals(1L, OutboxReport.collect(reader, "public", ">", "", 5, 0).get("audit_eligible"));
      var audit = OutboxReport.inspect(reader, "public", 5, Long.MIN_VALUE, "", 10).getFirst();
      assertEquals("ephemeral-login", audit.get("rule_id"));
      assertFalse(audit.containsKey("payload"));
      assertTrue(
          OutboxReport.inspect(
                  reader,
                  "public",
                  5,
                  ((Number) audit.get("discarded_at")).longValue(),
                  audit.get("id").toString(),
                  10)
              .isEmpty());
      try (var connection = reader.getConnection();
          var statement = connection.createStatement()) {
        assertThrows(
            SQLException.class, () -> statement.executeQuery("SELECT payload FROM kc_nats_outbox"));
        assertThrows(
            SQLException.class, () -> statement.executeUpdate("DELETE FROM kc_nats_discard_audit"));
      }
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                String metrics = metrics();
                assertEquals(1, metric(metrics, "knd_publication_discards_total"));
                assertEquals(
                    Boolean.TRUE.equals(audit.get("publication_may_have_occurred")) ? 1 : 0,
                    metric(metrics, "knd_publication_discards_unknown_total"));
                assertEquals(0, metric(metrics, "knd_publication_confirmed_total"));
                assertTrue(metric(metrics, "knd_publication_failures_total") > 0);
                assertTrue(metric(metrics, "knd_audit_cleanup_last_success_timestamp_seconds") > 0);
              });

      execute("ALTER TABLE kc_nats_discard_audit RENAME TO unavailable_audit");
      try {
        await()
            .atMost(Duration.ofSeconds(10))
            .until(() -> metric(metrics(), "knd_audit_cleanup_failures_total") > 0);
      } finally {
        execute("ALTER TABLE unavailable_audit RENAME TO kc_nats_discard_audit");
      }

      restartKeycloak();
      assertEquals(2, scalar("SELECT count(*) FROM kc_nats_outbox"));
      execute("UPDATE kc_nats_discard_audit SET discarded_at = discarded_at - 8*86400000::bigint");
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> scalar("SELECT count(*) FROM kc_nats_discard_audit") == 0);
      assertEquals(2, scalar("SELECT count(*) FROM kc_nats_outbox"));
    } finally {
      docker.startContainerCmd(broker.getContainerId()).exec();
    }
    connectNats();
    drained();
    assertEquals(2, messages(), "Only the protected Keycloak originals should reach NATS");
    for (int sequence = 1; sequence <= 2; sequence++) {
      var message = nats.jetStreamManagement().getMessage(STREAM, sequence);
      var event = objectMapper.readTree(message.getData());
      assertEquals(user, event.path("data").path("userId").asText());
      assertEquals(
          Integer.toString(sequence),
          event.path("data").path("ordering").path("sequence").asText());
      assertTrue(
          message
              .getSubject()
              .matches("keycloak\\.events\\.[^.]+\\.admin\\.user\\.(create|update)"));
    }
    assertTrue(nats.jetStreamManagement().getConsumerNames(STREAM).isEmpty());
    assertEquals(0L, OutboxReport.collect(reader, "public", ">", "", 5).get("pending"));
    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> metric(metrics(), "knd_publication_confirmed_total") == 2);
  }

  private static double metric(String text, String name) {
    return text.lines()
        .filter(line -> line.startsWith(name + " ") || line.startsWith(name + "{"))
        .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
        .sum();
  }

  private static String metrics() throws Exception {
    return httpClient
        .send(
            HttpRequest.newBuilder(
                    URI.create(
                        "http://"
                            + keycloak.getHost()
                            + ":"
                            + currentPort(keycloak, 9000)
                            + "/metrics"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString())
        .body();
  }
}
