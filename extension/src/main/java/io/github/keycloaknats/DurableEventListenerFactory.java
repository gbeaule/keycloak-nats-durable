package io.github.keycloaknats;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.utils.KeycloakModelUtils;
import org.keycloak.models.utils.PostMigrationEvent;

/** Registers request listeners and owns one relay worker for this Keycloak node. */
public final class DurableEventListenerFactory implements EventListenerProviderFactory {
  private static final Logger logger = Logger.getLogger(DurableEventListenerFactory.class);
  private BridgeConfig config;
  private ExecutorService executor;
  private final RelayWakeup wakeup = new RelayWakeup();
  private OutboxRelay relay;
  private EventPublisher publisher;
  private boolean closed;

  @Override
  public void init(Config.Scope scope) {
    config = BridgeConfig.from(scope);
  }

  @Override
  public String getId() {
    return "nats-durable";
  }

  @Override
  public EventListenerProvider create(KeycloakSession session) {
    return new DurableEventListener(session, config, wakeup::signal);
  }

  @Override
  public void postInit(KeycloakSessionFactory factory) {
    factory.register(
        event -> {
          if (event instanceof PostMigrationEvent) {
            start(factory);
          }
        });
  }

  private synchronized void start(KeycloakSessionFactory factory) {
    if (executor != null || closed) {
      return;
    }
    publisher = new JetStreamPublisher(config);
    relay =
        new OutboxRelay(
            work ->
                KeycloakModelUtils.runJobInTransactionWithResult(
                    factory,
                    session ->
                        work.apply(
                            session.getProvider(JpaConnectionProvider.class).getEntityManager())),
            publisher,
            config);
    executor =
        Executors.newSingleThreadExecutor(
            work -> {
              Thread thread = new Thread(work, "keycloak-nats-outbox");
              thread.setDaemon(true);
              return thread;
            });
    executor.execute(new RelayWorker(relay, wakeup, config));
    logger.infof("NATS durable outbox relay started; stream=%s", config.stream());
  }

  @Override
  public void close() {
    ExecutorService stoppingExecutor;
    EventPublisher stoppingPublisher;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      if (relay != null) {
        relay.stop();
      }
      wakeup.close();
      stoppingExecutor = executor;
      stoppingPublisher = publisher;
    }
    // Do not hold the lifecycle monitor while closing sockets or waiting for the worker.
    if (stoppingExecutor != null) {
      stoppingExecutor.shutdownNow();
    }
    if (stoppingPublisher != null) {
      stoppingPublisher.close();
    }
    if (stoppingExecutor != null) {
      try {
        if (!stoppingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          logger.warn("NATS relay shutdown is still completing; database rows remain recoverable");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
