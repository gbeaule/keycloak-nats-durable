package io.github.keycloaknats;

import io.nats.client.Connection;
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
  private static final int RECONNECT_FOREVER = -1;

  @FunctionalInterface
  interface Connector {
    Connection connect(Options options) throws IOException, InterruptedException;
  }

  private final BridgeConfig config;
  private final Connector connector;
  private final Object publishLock = new Object();
  private final Object lifecycleLock = new Object();
  // Guarded by lifecycleLock. Network operations must never hold that lock.
  private Connection connection;
  private boolean closed;

  /** Creates a lazy connection; broker outages never prevent listener initialization. */
  public JetStreamPublisher(BridgeConfig config) {
    this(config, Nats::connect);
  }

  JetStreamPublisher(BridgeConfig config, Connector connector) {
    this.config = config;
    this.connector = connector;
  }

  @Override
  public void publish(OutboxEvent event) throws Exception {
    // Serializes publication without preventing close() from cancelling a network request.
    synchronized (publishLock) {
      Connection activeConnection = connectionForPublish();
      if (activeConnection.getStatus() != Connection.Status.CONNECTED) {
        throw new IOException("NATS is unavailable");
      }
      JetStreamOptions options =
          JetStreamOptions.builder().requestTimeout(config.timeout()).build();
      StreamSafety.validate(
          activeConnection
              .jetStreamManagement(options)
              .getStreamInfo(config.stream())
              .getConfiguration(),
          config);
      Headers headers = new Headers().add("Content-Type", "application/cloudevents+json");
      PublishOptions publishOptions =
          PublishOptions.builder().expectedStream(config.stream()).messageId(event.id()).build();
      PublishAck ack =
          activeConnection
              .jetStream(options)
              .publish(
                  event.subject(),
                  headers,
                  event.payload().getBytes(StandardCharsets.UTF_8),
                  publishOptions);
      if (!config.stream().equals(ack.getStream()) || ack.getSeqno() < 1) {
        throw new IOException("Unexpected publish acknowledgement");
      }
    }
  }

  private Connection connectionForPublish() throws IOException, InterruptedException {
    synchronized (lifecycleLock) {
      if (closed) {
        throw new IOException("Publisher is closed");
      }
      if (connection != null && connection.getStatus() != Connection.Status.CLOSED) {
        return connection;
      }
    }

    Connection candidate = connector.connect(connectionOptions());
    synchronized (lifecycleLock) {
      if (!closed) {
        connection = candidate;
        return candidate;
      }
    }
    // A connect already in progress can finish after shutdown. Never install or leak it.
    closeConnection(candidate);
    throw new IOException("Publisher closed while connecting");
  }

  private Options connectionOptions() throws IOException {
    NatsDiagnostics diagnostics = new NatsDiagnostics();
    Options.Builder options =
        new Options.Builder()
            .servers(config.servers())
            .connectionName("keycloak-nats-durable")
            .connectionTimeout(config.timeout())
            .maxReconnects(RECONNECT_FOREVER)
            .reconnectBufferSize(0)
            .errorListener(diagnostics)
            .connectionListener(diagnostics);
    if (config.credentialsFile() != null) {
      options.authHandler(Nats.credentials(config.credentialsFile()));
    }
    if (config.token() != null) {
      options.token(config.token().toCharArray());
    }
    if (config.tls().enabled()) {
      try {
        options.sslContext(config.tls().createContext());
        // Preserve the configured DNS identity for SNI and certificate hostname verification.
        options.hostnameResolveMode(Options.HostnameResolveMode.HappyEyeballs);
      } catch (java.security.GeneralSecurityException e) {
        throw new IOException("Cannot load verified NATS TLS configuration");
      }
    }
    return options.build();
  }

  @Override
  public void close() {
    Connection detached;
    synchronized (lifecycleLock) {
      closed = true;
      detached = connection;
      connection = null;
    }
    closeConnection(detached);
  }

  private static void closeConnection(Connection connection) {
    if (connection == null) {
      return;
    }
    try {
      connection.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
