package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Verifies schema-qualified installation, migration reuse and delivery outside public. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class CustomSchemaIT extends IntegrationSupport {
  private static final String TABLE = "\"bridge-data\".kc_nats_outbox";

  @BeforeAll
  static void start() throws Exception {
    startInfrastructure(
        System.getProperty("keycloak.version"),
        container -> {
          try {
            execute("CREATE SCHEMA \"bridge-data\"");
          } catch (Exception failure) {
            throw new IllegalStateException(failure);
          }
          container
              .withEnv("KC_DB_SCHEMA", "bridge-data")
              .withEnv("KND_AUDIT_CLEANUP_INTERVAL_MS", "1000")
              .withEnv("KC_METRICS_ENABLED", "true")
              .withExposedPorts(8080, 9000);
        });
  }

  @AfterAll
  static void stop() throws Exception {
    stopInfrastructure();
  }

  @Test
  void installsInQuotedSchemaAndPreservesPendingEventsAcrossRestart() throws Exception {
    assertEquals(
        1,
        scalar(
            """
            SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'bridge-data' AND c.relname = 'kc_nats_outbox'
              AND c.reloptions @> ARRAY['autovacuum_vacuum_scale_factor=0.02']
            """));
    assertEquals(
        0,
        scalar(
            """
            SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = 'kc_nats_outbox'
            """));
    var docker = broker.getDockerClient();
    docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    try {
      createUser();
      assertEquals(1, scalar("SELECT count(*) FROM " + TABLE));
      assertEquals(
          1,
          scalar(
              "SELECT count(*) FROM "
                  + TABLE
                  + " WHERE user_sequence=1"
                  + " AND ordering_key IS NOT NULL AND filter_sha256 IS NOT NULL"));
      assertEquals(1, scalar("SELECT count(*) FROM \"bridge-data\".kc_nats_capture_counter"));
      assertEquals(0, scalar("SELECT count(*) FROM \"bridge-data\".kc_nats_discard_audit"));
      insertOldAudit();
      await()
          .atMost(Duration.ofSeconds(15))
          .until(() -> scalar("SELECT count(*) FROM \"bridge-data\".kc_nats_discard_audit") == 0);
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                String metrics =
                    httpClient
                        .send(
                            HttpRequest.newBuilder(
                                    URI.create(
                                        "http://"
                                            + keycloak.getHost()
                                            + ":"
                                            + keycloak.getMappedPort(9000)
                                            + "/metrics"))
                                .GET()
                                .build(),
                            HttpResponse.BodyHandlers.ofString())
                        .body();
                org.junit.jupiter.api.Assertions.assertTrue(
                    metrics.contains("knd_audit_cleanup_deleted_total 1.0"), metrics);
                org.junit.jupiter.api.Assertions.assertTrue(
                    metrics.contains("knd_audit_retained_rows 0.0"), metrics);
              });
      restartKeycloak();
      assertEquals(1, scalar("SELECT count(*) FROM " + TABLE));
      insertOldAudit();
      await()
          .atMost(Duration.ofSeconds(15))
          .until(() -> scalar("SELECT count(*) FROM \"bridge-data\".kc_nats_discard_audit") == 0);
      docker.startContainerCmd(broker.getContainerId()).exec();
      connectNats();
      await()
          .atMost(Duration.ofSeconds(20))
          .until(() -> scalar("SELECT count(*) FROM " + TABLE) == 0);
      assertEquals(1, messages());
      assertEquals(1, scalar("SELECT count(*) FROM \"bridge-data\".kc_nats_capture_counter"));
    } finally {
      if (!broker.isRunning()) {
        docker.startContainerCmd(broker.getContainerId()).exec();
      }
    }
  }

  private static void insertOldAudit() throws Exception {
    execute(
        """
        INSERT INTO "bridge-data".kc_nats_discard_audit
          (id,realm_id,event_type,subject,payload_sha256,created_at,filter_sha256,
           max_age_seconds,expires_at,discarded_at,reason,attempts,publication_may_have_occurred)
        SELECT 'old-audit',realm_id,event_type,subject,payload_sha256,0,filter_sha256,
          1,1000,1000,'EXPIRED',0,false FROM "bridge-data".kc_nats_outbox LIMIT 1
        """);
    assertEquals(1, scalar("SELECT count(*) FROM " + TABLE));
  }
}
