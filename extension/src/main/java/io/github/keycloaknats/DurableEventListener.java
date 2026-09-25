package io.github.keycloaknats;

import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;

/** Captures an allowlisted event in the same database transaction as the Keycloak operation. */
public final class DurableEventListener implements EventListenerProvider {
  private final KeycloakSession session;
  private final EventEnvelope envelopes;
  private final Runnable wakeRelay;
  private boolean wakeupEnlisted;

  /** Binds capture to this request's session and a notification issued only after commit. */
  public DurableEventListener(KeycloakSession session, BridgeConfig config, Runnable wakeRelay) {
    this.session = session;
    this.envelopes = new EventEnvelope(config);
    this.wakeRelay = wakeRelay;
  }

  @Override
  public void onEvent(Event event) {
    persist(() -> envelopes.user(event));
  }

  @Override
  public void onEvent(AdminEvent event, boolean includeRepresentation) {
    persist(() -> envelopes.admin(event, enabledState(event)));
  }

  private Boolean enabledState(AdminEvent event) {
    String userId = EventEnvelope.targetUserId(event);
    OperationType operation = event.getOperationType();
    boolean observesUserState =
        operation == OperationType.CREATE || operation == OperationType.UPDATE;
    if (userId == null || event.getError() != null || !observesUserState) {
      return null;
    }
    RealmModel realm = session.realms().getRealm(event.getRealmId());
    if (realm == null) {
      return null;
    }
    UserModel user = session.users().getUserById(realm, userId);
    return user == null ? null : user.isEnabled();
  }

  private void persist(java.util.function.Supplier<OutboxEvent> event) {
    try {
      if (!session.getTransactionManager().isActive()) {
        throw new IllegalStateException("An active Keycloak transaction is required");
      }
      var em = session.getProvider(JpaConnectionProvider.class).getEntityManager();
      em.persist(event.get());
      // Detect database rejection while the request is still inside the listener boundary.
      em.flush();
      enlistWakeup();
    } catch (RuntimeException | Error failure) {
      // Keycloak catches listener failures. Throwing alone would silently commit the account
      // change.
      session.getTransactionManager().setRollbackOnly();
      throw failure;
    }
  }

  private void enlistWakeup() {
    if (wakeupEnlisted) {
      return;
    }
    session
        .getTransactionManager()
        .enlistAfterCompletion(
            new AbstractKeycloakTransaction() {
              @Override
              protected void commitImpl() {
                wakeupEnlisted = false;
                wakeRelay.run();
              }

              @Override
              protected void rollbackImpl() {
                wakeupEnlisted = false;
              }
            });
    wakeupEnlisted = true;
  }

  @Override
  public void close() {}
}
