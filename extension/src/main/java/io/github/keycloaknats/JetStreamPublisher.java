package io.github.keycloaknats;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStreamOptions;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Connection failures never turn capture off; the relay retries from the database. */
public final class JetStreamPublisher implements EventPublisher {
  private final BridgeConfig config;
  private Connection connection;
  private boolean closed;

  public JetStreamPublisher(BridgeConfig config) {
    this.config = config;
  }

  @Override
  public synchronized void publish(OutboxEvent event) throws Exception {
    if (closed) throw new IOException("Publisher is closed");
    if (connection == null || connection.getStatus() == Connection.Status.CLOSED) {
      Options.Builder options =
          new Options.Builder()
              .servers(config.servers())
              .connectionName("keycloak-nats-durable")
              .connectionTimeout(config.timeout())
              .maxReconnects(-1)
              .reconnectBufferSize(0)
              .errorListener(new ErrorListener() {});
      if (config.credentialsFile() != null)
        options.authHandler(Nats.credentials(config.credentialsFile()));
      if (config.token() != null) options.token(config.token().toCharArray());
      connection = Nats.connect(options.build());
    }
    if (connection.getStatus() != Connection.Status.CONNECTED)
      throw new IOException("NATS is unavailable");
    JetStreamOptions options = JetStreamOptions.builder().requestTimeout(config.timeout()).build();
    StreamSafety.validate(
        connection.jetStreamManagement(options).getStreamInfo(config.stream()).getConfiguration(),
        config);
    Headers headers = new Headers().add("Content-Type", "application/cloudevents+json");
    PublishOptions publishOptions =
        PublishOptions.builder().expectedStream(config.stream()).messageId(event.id()).build();
    PublishAck ack =
        connection
            .jetStream(options)
            .publish(
                event.subject(),
                headers,
                event.payload().getBytes(StandardCharsets.UTF_8),
                publishOptions);
    if (!config.stream().equals(ack.getStream()) || ack.getSeqno() < 1)
      throw new IOException("Unexpected publish acknowledgement");
  }

  @Override
  public synchronized void close() {
    closed = true;
    if (connection != null) {
      try {
        connection.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
