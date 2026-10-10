package io.github.gbeaule.keycloaknats;

import io.github.gbeaule.keycloaknats.jetstream.StreamPolicy;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamOptions;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

/** Connection failures never turn capture off; the relay retries from the database. */
public final class JetStreamPublisher implements EventPublisher {
  private static final int RECONNECT_FOREVER = -1;

  @FunctionalInterface
  interface Connector {
    Connection connect(Options options) throws IOException, InterruptedException;
  }

  private final BridgeConfig config;
  private final Connector connector;
  private final LongSupplier nanoTime;
  private final Object publishLock = new Object();
  private final Object lifecycleLock = new Object();
  // Guarded by lifecycleLock. Network operations must never hold that lock.
  private Connection connection;
  private Attempt activeAttempt;
  private int closingConnections;
  private boolean closed;

  private final class Attempt {
    private final long started = nanoTime.getAsLong();
    private final CompletableFuture<Void> result = new CompletableFuture<>();
    private final Thread thread;
    private volatile boolean cancelled;

    Attempt(OutboxEvent event) {
      thread =
          Thread.ofVirtual()
              .name("keycloak-nats-publication")
              .unstarted(() -> runAttempt(event, this));
    }

    Duration remaining() throws IOException {
      if (cancelled) {
        throw new IOException("Publication attempt cancelled");
      }
      long remaining = config.timeout().toNanos() - (nanoTime.getAsLong() - started);
      if (remaining <= 0) {
        throw new SocketTimeoutException("Publication attempt deadline exceeded");
      }
      return Duration.ofNanos(remaining);
    }
  }

  /** Creates a lazy connection; broker outages never prevent listener initialization. */
  public JetStreamPublisher(BridgeConfig config) {
    this(config, Nats::connect);
  }

  JetStreamPublisher(BridgeConfig config, Connector connector) {
    this(config, connector, System::nanoTime);
  }

  JetStreamPublisher(BridgeConfig config, Connector connector, LongSupplier nanoTime) {
    this.config = config;
    this.connector = connector;
    this.nanoTime = nanoTime;
  }

  @Override
  public void publish(OutboxEvent event) throws Exception {
    // A caller retains database ownership only for this total budget, even if DNS or a library
    // operation ignores interruption. One unfinished task per publisher bounds cancellation work.
    synchronized (publishLock) {
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException("Publication interrupted");
      }
      Attempt attempt;
      synchronized (lifecycleLock) {
        if (closed) {
          throw new IOException("Publisher is closed");
        }
        if (activeAttempt != null || closingConnections != 0) {
          throw new IOException("Previous publication cancellation is still completing");
        }
        attempt = new Attempt(event);
        activeAttempt = attempt;
      }
      attempt.thread.start();
      try {
        attempt.result.get(attempt.remaining().toNanos(), TimeUnit.NANOSECONDS);
      } catch (TimeoutException | SocketTimeoutException timeout) {
        cancel(attempt);
        throw new SocketTimeoutException("Publication attempt deadline exceeded");
      } catch (InterruptedException interrupted) {
        cancel(attempt);
        throw interrupted;
      } catch (ExecutionException failed) {
        if (failed.getCause() instanceof Error error) {
          throw error;
        }
        throw (Exception) failed.getCause();
      }
    }
  }

  private void runAttempt(OutboxEvent event, Attempt attempt) {
    Throwable failure = null;
    try {
      publishWithinBudget(event, attempt);
    } catch (Throwable failed) {
      // Transfer failures across the task boundary; the relay retains its normal error policy.
      failure = failed;
    } finally {
      synchronized (lifecycleLock) {
        activeAttempt = null;
      }
    }
    if (failure == null) {
      attempt.result.complete(null);
    } else {
      attempt.result.completeExceptionally(failure);
    }
  }

  private void publishWithinBudget(OutboxEvent event, Attempt attempt) throws Exception {
    Connection activeConnection = connectionForPublish(attempt);
    if (activeConnection.getStatus() != Connection.Status.CONNECTED) {
      throw new IOException("NATS is unavailable");
    }
    StreamPolicy.validateServerPayload(activeConnection.getMaxPayload(), config.maxPayloadBytes());
    JetStreamOptions options =
        JetStreamOptions.builder().requestTimeout(attempt.remaining()).build();
    StreamPolicy.validate(
        activeConnection
            .jetStreamManagement(options)
            .getStreamInfo(config.stream())
            .getConfiguration(),
        config.stream(),
        config.subjectPrefix(),
        config.minReplicas(),
        config.maxPayloadBytes());
    Headers headers = new Headers().add("Content-Type", "application/cloudevents+json");
    PublishOptions publishOptions =
        PublishOptions.builder().expectedStream(config.stream()).messageId(event.id()).build();
    JetStream stream =
        activeConnection.jetStream(
            JetStreamOptions.builder().requestTimeout(attempt.remaining()).build());
    byte[] payload = event.payload().getBytes(StandardCharsets.UTF_8);
    attempt.remaining();
    PublishAck ack = stream.publish(event.subject(), headers, payload, publishOptions);
    if (!config.stream().equals(ack.getStream()) || ack.getSeqno() < 1) {
      throw new IOException("Unexpected publish acknowledgement");
    }
  }

  private Connection connectionForPublish(Attempt attempt)
      throws IOException, InterruptedException {
    synchronized (lifecycleLock) {
      attempt.remaining();
      if (connection != null && connection.getStatus() != Connection.Status.CLOSED) {
        return connection;
      }
    }

    Options options = connectionOptions(attempt);
    attempt.remaining();
    Connection candidate = connector.connect(options);
    synchronized (lifecycleLock) {
      if (!closed && !attempt.cancelled) {
        connection = candidate;
        return candidate;
      }
    }
    // A connect already in progress can finish after shutdown. Never install or leak it.
    closeConnection(candidate);
    throw new IOException("Publisher closed while connecting");
  }

  private Options connectionOptions(Attempt attempt) throws IOException {
    NatsDiagnostics diagnostics = new NatsDiagnostics();
    Options.Builder options =
        new Options.Builder()
            .servers(config.servers())
            .connectionName("keycloak-nats-durable")
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
    return options.connectionTimeout(attempt.remaining()).build();
  }

  @Override
  public void close() {
    Connection detached;
    Attempt cancelling;
    synchronized (lifecycleLock) {
      closed = true;
      cancelling = activeAttempt;
      if (cancelling != null) {
        cancelling.cancelled = true;
      }
      detached = connection;
      connection = null;
      if (detached != null) {
        closingConnections++;
      }
    }
    if (cancelling != null) {
      cancelling.result.completeExceptionally(new IOException("Publisher is closed"));
      cancelling.thread.interrupt();
    }
    closeDetached(detached);
  }

  private void cancel(Attempt attempt) {
    Connection detached;
    synchronized (lifecycleLock) {
      attempt.cancelled = true;
      detached = connection;
      connection = null;
      if (detached != null) {
        closingConnections++;
      }
    }
    attempt.thread.interrupt();
    closeDetached(detached);
  }

  private void closeDetached(Connection detached) {
    if (detached != null) {
      // Socket cleanup cannot extend the caller's attempt or shutdown deadline. While cleanup is
      // unfinished, this publisher accepts no new attempts and cannot accumulate cleanup tasks.
      Thread.ofVirtual()
          .name("keycloak-nats-connection-close")
          .start(
              () -> {
                try {
                  closeConnection(detached);
                } finally {
                  synchronized (lifecycleLock) {
                    closingConnections--;
                  }
                }
              });
    }
  }

  private static void closeConnection(Connection connection) {
    try {
      connection.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
