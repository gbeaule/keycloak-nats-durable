package io.github.keycloaknats;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

public final class DurableEventListenerFactory implements EventListenerProviderFactory {
  private static final Logger LOG = Logger.getLogger(DurableEventListenerFactory.class);
  private BridgeConfig config;
  private ScheduledExecutorService executor;
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
    return new DurableEventListener(session, config);
  }

  @Override
  public void postInit(KeycloakSessionFactory factory) {
    factory.register(
        event -> {
          if (event instanceof PostMigrationEvent) start(factory);
        });
  }

  private synchronized void start(KeycloakSessionFactory factory) {
    if (executor != null || closed) return;
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
        Executors.newSingleThreadScheduledExecutor(
            work -> {
              Thread thread = new Thread(work, "keycloak-nats-outbox");
              thread.setDaemon(true);
              return thread;
            });
    executor.scheduleWithFixedDelay(
        relay,
        config.pollInterval().toMillis(),
        config.pollInterval().toMillis(),
        TimeUnit.MILLISECONDS);
    LOG.infof("NATS durable outbox relay started; stream=%s", config.stream());
  }

  @Override
  public synchronized void close() {
    closed = true;
    if (relay != null) relay.stop();
    if (executor != null) executor.shutdownNow();
    if (publisher != null) publisher.close();
    if (executor != null) {
      try {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS))
          LOG.warn("NATS relay shutdown is still completing; database rows remain recoverable");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
