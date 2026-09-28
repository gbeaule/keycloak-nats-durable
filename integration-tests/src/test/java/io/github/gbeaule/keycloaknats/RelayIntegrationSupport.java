package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.nats.client.api.MessageInfo;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/** PostgreSQL and JetStream fixtures for relay ownership and discard tests. */
abstract class RelayIntegrationSupport extends IntegrationSupport {
  static SessionFactory sessions;
  static BridgeConfig config;

  @BeforeAll
  static void start() throws Exception {
    startInfrastructure(
        System.getProperty("keycloak.version"),
        container -> {
          try {
            execute("CREATE SCHEMA \"relay-data\"");
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
          container.withEnv("KC_DB_SCHEMA", "relay-data");
        },
        false);
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> scalar("SELECT count(*) FROM \"relay-data\".kc_nats_outbox") == 0);
    // Keep the installed schema but control every worker and transaction from the test.
    keycloak.stop();
    sessions =
        new Configuration()
            .addAnnotatedClass(OutboxEvent.class)
            .addAnnotatedClass(CaptureCounter.class)
            .addAnnotatedClass(DiscardAudit.class)
            .setProperty("hibernate.connection.url", postgres.getJdbcUrl())
            .setProperty("hibernate.connection.username", postgres.getUsername())
            .setProperty("hibernate.connection.password", postgres.getPassword())
            .setProperty("hibernate.default_schema", "\"relay-data\"")
            .setProperty("hibernate.hbm2ddl.auto", "validate")
            .buildSessionFactory();
  }

  @BeforeEach
  void reset() throws Exception {
    // Docker may assign a new host port when a test restarts the broker.
    config =
        BridgeConfig.from(
            Map.of(
                "nats-url",
                natsUrl(),
                "min-replicas",
                "1",
                "timeout-ms",
                "1000",
                "retry-initial-ms",
                "60000",
                "retry-max-ms",
                "60000"));
    transaction(
        em -> {
          em.createQuery("delete from NatsOutboxEvent").executeUpdate();
          em.createQuery("delete from NatsDiscardAudit").executeUpdate();
        });
    nats.jetStreamManagement().deleteStream(STREAM);
    provisionStream();
  }

  @AfterAll
  static void stop() throws Exception {
    if (sessions != null) {
      sessions.close();
    }
    stopInfrastructure();
  }

  static void transaction(Consumer<EntityManager> work) {
    sessions.inTransaction(work::accept);
  }

  static void capture(String id, String user) {
    transaction(em -> capture(em, id, "relay", user));
  }

  static void capture(EntityManager em, String id, String realm, String user) {
    var policy =
        new ResolvedPublicationPolicy(
            PublicationPolicy.RETRY, EventFilter.digest(new byte[0]), null);
    em.persist(
        new OutboxEvent(
            id,
            "keycloak.events.relay." + id,
            "{\"id\":\"" + id + "\",\"value\":\"é\"}",
            CaptureRepository.databaseTime(em),
            realm,
            "io.keycloak.user.login",
            CaptureRepository.next(em, realm, user),
            policy));
  }

  static OutboxEvent row(String id) {
    try (var session = sessions.openSession()) {
      return session.find(OutboxEvent.class, id);
    }
  }

  static List<MessageInfo> storedMessages(int count) throws Exception {
    var management = nats.jetStreamManagement();
    var state = management.getStreamInfo(STREAM).getStreamState();
    assertEquals(count, state.getMsgCount());
    var messages = new java.util.ArrayList<MessageInfo>();
    for (long sequence = state.getFirstSequence();
        sequence <= state.getLastSequence();
        sequence++) {
      messages.add(management.getMessage(STREAM, sequence));
    }
    return messages;
  }

  static List<String> storedIds(int count) throws Exception {
    var delivered = storedMessages(count);
    assertEquals(count, delivered.size());
    var ids = new java.util.ArrayList<String>();
    for (var message : delivered) {
      ids.add(objectMapper.readTree(message.getData()).path("id").asText());
    }
    return ids;
  }

  @FunctionalInterface
  interface Send {
    void publish(OutboxEvent event) throws Exception;
  }

  static EventPublisher publisher(Send send) {
    return new EventPublisher() {
      @Override
      public void publish(OutboxEvent event) throws Exception {
        send.publish(event);
      }

      @Override
      public void close() {}
    };
  }
}
