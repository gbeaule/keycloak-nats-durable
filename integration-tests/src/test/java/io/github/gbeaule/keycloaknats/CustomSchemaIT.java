package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
          container.withEnv("KC_DB_SCHEMA", "bridge-data");
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
      restartKeycloak();
      assertEquals(1, scalar("SELECT count(*) FROM " + TABLE));
      docker.startContainerCmd(broker.getContainerId()).exec();
      connectNats();
      await()
          .atMost(Duration.ofSeconds(20))
          .until(() -> scalar("SELECT count(*) FROM " + TABLE) == 0);
      assertEquals(1, messages());
    } finally {
      if (!broker.isRunning()) {
        docker.startContainerCmd(broker.getContainerId()).exec();
      }
    }
  }
}
