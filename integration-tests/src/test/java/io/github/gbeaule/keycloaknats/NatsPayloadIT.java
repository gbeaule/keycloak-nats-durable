package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.gbeaule.keycloaknats.jetstream.StreamPolicy;
import io.nats.client.Nats;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

/** Negotiated server limits, including real JetStream headers at the supported payload boundary. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class NatsPayloadIT {
  @ParameterizedTest
  @ValueSource(ints = {1048576, 2097152})
  void validatesHeaderRoomAndPublishesTheUnchangedMaximumEnvelope(int serverLimit)
      throws Exception {
    try (var broker =
        new GenericContainer<>(IntegrationSupport.NATS_IMAGE)
            .withExposedPorts(4222)
            .withCopyToContainer(
                Transferable.of("jetstream {store_dir: /data}\nmax_payload: " + serverLimit),
                "/etc/nats/test.conf")
            .withCommand("-c", "/etc/nats/test.conf")) {
      broker.start();
      String url = "nats://" + broker.getHost() + ":" + broker.getMappedPort(4222);
      try (var nats = Nats.connect(url)) {
        nats.jetStreamManagement()
            .addStream(
                StreamConfiguration.builder()
                    .name("KEYCLOAK_EVENTS")
                    .subjects("keycloak.events.>")
                    .storageType(StorageType.File)
                    .retentionPolicy(RetentionPolicy.WorkQueue)
                    .discardPolicy(DiscardPolicy.New)
                    .replicas(1)
                    .duplicateWindow(Duration.ofMinutes(2))
                    .build());
        var config =
            BridgeConfig.from(
                Map.of("nats-url", url, "min-replicas", "1", "max-payload-bytes", "1048576"));
        var envelopes = new EventEnvelope(config);
        var event = new Event();
        event.setRealmId("realm");
        event.setType(EventType.LOGIN_ERROR);
        event.setError("");
        var policy =
            new ResolvedPublicationPolicy(
                PublicationPolicy.RETRY, EventFilter.all().sha256(), null);
        int base =
            envelopes
                .serialize(envelopes.describe(event), null, 1, policy)
                .payload()
                .getBytes(StandardCharsets.UTF_8)
                .length;
        event.setError("x".repeat(config.maxPayloadBytes() - base));
        var row = envelopes.serialize(envelopes.describe(event), null, 1, policy);
        assertEquals(1048576, row.payload().getBytes(StandardCharsets.UTF_8).length);
        try (var publisher = new JetStreamPublisher(config)) {
          if (serverLimit == 1048576) {
            var failure = assertThrows(StreamPolicy.Violation.class, () -> publisher.publish(row));
            assertEquals(
                "Server max_payload must leave room for the configured payload and headers",
                failure.getMessage());
            assertEquals(
                0,
                nats.jetStreamManagement()
                    .getStreamInfo("KEYCLOAK_EVENTS")
                    .getStreamState()
                    .getMsgCount());
          } else {
            publisher.publish(row);
            assertEquals(
                row.payload(),
                new String(
                    nats.jetStreamManagement().getMessage("KEYCLOAK_EVENTS", 1).getData(),
                    StandardCharsets.UTF_8));
          }
        }
      }
    }
  }
}
