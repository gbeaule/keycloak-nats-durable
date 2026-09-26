package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.nats.client.Options;
import java.util.function.Consumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;

/** Exercises the interval between Docker starting a container and NATS accepting connections. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class NatsReadinessIT extends IntegrationSupport {
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
        provision();
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
