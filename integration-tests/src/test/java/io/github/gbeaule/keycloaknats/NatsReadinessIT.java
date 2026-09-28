package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.nats.client.Options;
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
