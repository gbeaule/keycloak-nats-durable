package io.github.gbeaule.keycloaknats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.utils.KeycloakModelUtils;
import org.keycloak.models.utils.PostMigrationEvent;

/** Registers request listeners and owns a bounded relay pool for this Keycloak node. */
public final class DurableEventListenerFactory implements EventListenerProviderFactory {
  private static final Logger logger = Logger.getLogger(DurableEventListenerFactory.class);
  private BridgeConfig config;
  private ReloadingEventFilter filter;
  private ExecutorService executor;
  private final List<RelayWakeup> wakeups = new CopyOnWriteArrayList<>();
  private final List<OutboxRelay> relays = new ArrayList<>();
  private final List<EventPublisher> publishers = new ArrayList<>();
  private boolean closed;

  @Override
  public void init(Config.Scope scope) {
    config = BridgeConfig.from(scope);
    filter = new ReloadingEventFilter(config.filterFile());
  }

  @Override
  public String getId() {
    return "nats-durable";
  }

  @Override
  public EventListenerProvider create(KeycloakSession session) {
    return new DurableEventListener(
        session, config, () -> wakeups.forEach(RelayWakeup::signal), filter::current);
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
    filter.start(config.filterReloadInterval().toMillis());
    AtomicInteger workerNumber = new AtomicInteger();
    executor =
        Executors.newFixedThreadPool(
            config.relayWorkers(),
            work -> {
              Thread thread =
                  new Thread(work, "keycloak-nats-outbox-" + workerNumber.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });
    for (int i = 0; i < config.relayWorkers(); i++) {
      EventPublisher publisher = new JetStreamPublisher(config);
      RelayWakeup wakeup = new RelayWakeup();
      OutboxRelay relay =
          new OutboxRelay(
              work ->
                  KeycloakModelUtils.runJobInTransaction(
                      factory,
                      session ->
                          work.accept(
                              session.getProvider(JpaConnectionProvider.class).getEntityManager())),
              publisher,
              config);
      publishers.add(publisher);
      wakeups.add(wakeup);
      relays.add(relay);
      executor.execute(new RelayWorker(relay, wakeup, config));
    }
    logger.infof(
        "NATS durable outbox relay started; stream=%s workers=%d",
        config.stream(), config.relayWorkers());
  }

  @Override
  public void close() {
    ExecutorService stoppingExecutor;
    List<EventPublisher> stoppingPublishers;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      relays.forEach(OutboxRelay::stop);
      wakeups.forEach(RelayWakeup::close);
      if (filter != null) {
        filter.close();
      }
      stoppingExecutor = executor;
      stoppingPublishers = List.copyOf(publishers);
    }
    // Do not hold the lifecycle monitor while closing sockets or waiting for the worker.
    if (stoppingExecutor != null) {
      stoppingExecutor.shutdownNow();
    }
    stoppingPublishers.forEach(EventPublisher::close);
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
