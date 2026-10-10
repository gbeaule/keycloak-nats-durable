package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.nats.client.Options;
import io.nats.client.api.PersistMode;
import io.nats.client.api.StreamConfiguration;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/** Exercises authenticated publication and reconnects while NATS is still starting. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class NatsReadinessIT extends IntegrationSupport {
  @Test
  void asyncPersistenceRetainsTheOutboxUntilTheStreamIsReplacedWithDefaultPersistence()
      throws Exception {
    try {
      startInfrastructure(System.getProperty("keycloak.version", "26.8.0"), container -> {}, false);
      drained();
      var safe = nats.jetStreamManagement().getStreamInfo(STREAM).getConfiguration();
      nats.jetStreamManagement().deleteStream(STREAM);
      nats.jetStreamManagement()
          .addStream(StreamConfiguration.builder(safe).persistMode(PersistMode.Async).build());
      assertEquals(
          PersistMode.Async,
          nats.jetStreamManagement().getStreamInfo(STREAM).getConfiguration().getPersistMode());

      String user = createUser();
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> scalar("SELECT count(*) FROM kc_nats_outbox WHERE attempts > 0") == 1);
      assertEquals(0, messages(), "An async stream must receive no publication from the provider");
      String id;
      String payload;
      try (var db = database.getConnection();
          var query = db.createStatement();
          var result = query.executeQuery("SELECT id,payload FROM kc_nats_outbox")) {
        assertTrue(result.next(), "Rejection must retain the original capture");
        id = result.getString(1);
        payload = result.getString(2);
        assertEquals(user, objectMapper.readTree(payload).at("/data/userId").asText());
        assertFalse(result.next());
      }

      // Persist mode is immutable: repair the destination by recreating the empty stream.
      nats.jetStreamManagement().deleteStream(STREAM);
      provisionStream();
      drained();
      assertEquals(1, messages());
      var message = nats.jetStreamManagement().getMessage(STREAM, 1);
      assertEquals(payload, new String(message.getData(), StandardCharsets.UTF_8));
      assertEquals(id, message.getHeaders().getFirst("Nats-Msg-Id"));
      assertTrue(nats.jetStreamManagement().getConsumerNames(STREAM).isEmpty());
    } finally {
      stopInfrastructure();
    }
  }

  @Test
  void publisherPreservesWhitespaceInAuthenticationTokens() throws Exception {
    String token = " integration-token ";
    network = Network.newNetwork();
    postgres =
        postgresContainer()
            .withDatabaseName("keycloak")
            .withUsername("keycloak")
            .withPassword("integration-password")
            .withNetwork(network)
            .withNetworkAliases("postgres");
    broker =
        new GenericContainer<>(NATS_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("nats")
            .withExposedPorts(4222)
            .withCommand("-js", "-sd", "/data", "--auth", token);
    keycloak = keycloakContainer(true).withEnv("KND_TOKEN", token);
    try {
      postgres.start();
      broker.start();
      database = new PGSimpleDataSource();
      database.setURL(postgres.getJdbcUrl());
      database.setUser(postgres.getUsername());
      database.setPassword(postgres.getPassword());
      connectNats(options -> options.token(token.toCharArray()));
      provisionStream();
      keycloak.start();
      loginAdmin();
      createUser();
      drained();
      assertEquals(1, messages());
    } finally {
      stopInfrastructure();
    }
  }

  @ParameterizedTest(name = "authenticated={0}")
  @ValueSource(booleans = {false, true})
  void reconnectWaitsForBrokerReadinessAndPreservesStoredMessages(boolean authenticated)
      throws Exception {
    String authentication = authenticated ? " --user readiness --pass integration-password" : "";
    Consumer<Options.Builder> configure =
        options -> {
          if (authenticated) {
            options.userInfo("readiness", "integration-password");
          }
        };
    try (var delayedBroker =
        new GenericContainer<>(NATS_IMAGE)
            .withExposedPorts(4222)
            // Delay the server, including after a raw Docker restart that bypasses startup waits.
            .withCommand("sh", "-c", "sleep 3; exec nats-server -js -sd /data" + authentication)) {
      broker = delayedBroker;
      broker.start();
      try {
        connectNats(configure);
        provisionStream();
        nats.jetStream().publish("keycloak.events.readiness", new byte[] {1});
        assertEquals(1, messages());

        var docker = broker.getDockerClient();
        docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
        docker.startContainerCmd(broker.getContainerId()).exec();
        connectNats(configure);

        assertEquals(1, messages(), "Reconnecting must retain the original JetStream data");
        nats.jetStream().publish("keycloak.events.readiness", new byte[] {2});
        assertEquals(2, messages());
      } finally {
        if (nats != null) {
          nats.close();
          nats = null;
        }
      }
    }
  }
}
