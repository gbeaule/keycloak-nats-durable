package io.github.gbeaule.keycloaknats;

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
  private final java.util.function.Supplier<EventFilter> filter;
  private boolean wakeupEnlisted;

  /** Binds capture to this request's session and a notification issued only after commit. */
  public DurableEventListener(KeycloakSession session, BridgeConfig config, Runnable wakeRelay) {
    this(session, config, wakeRelay, EventFilter::all);
  }

  DurableEventListener(
      KeycloakSession session,
      BridgeConfig config,
      Runnable wakeRelay,
      java.util.function.Supplier<EventFilter> filter) {
    this.session = session;
    this.envelopes = new EventEnvelope(config);
    this.wakeRelay = wakeRelay;
    this.filter = filter;
  }

  @Override
  public void onEvent(Event event) {
    persist(
        () ->
            filter
                .get()
                .resolve(event, envelopes.userSubject(event))
                .map(policy -> new Capture(envelopes.describe(event), policy))
                .orElse(null));
  }

  @Override
  public void onEvent(AdminEvent event, boolean includeRepresentation) {
    persist(
        () -> {
          EventFilter policy = filter.get();
          String subject = envelopes.adminSubject(event);
          if (!policy.mayAccept(event, subject)) {
            return null;
          }
          String userId = AffectedUser.resolve(event, session);
          Boolean enabled = enabledState(event, userId);
          return policy
              .resolve(event, userId, enabled, subject)
              .map(resolved -> new Capture(envelopes.describe(event, userId, enabled), resolved))
              .orElse(null);
        });
  }

  private Boolean enabledState(AdminEvent event, String userId) {
    OperationType operation = event.getOperationType();
    boolean observesUserState =
        operation == OperationType.CREATE || operation == OperationType.UPDATE;
    if (!AffectedUser.isDirectUser(event, userId)
        || event.getError() != null
        || !observesUserState) {
      return null;
    }
    RealmModel realm = session.realms().getRealm(event.getRealmId());
    if (realm == null) {
      return null;
    }
    UserModel user = session.users().getUserById(realm, userId);
    return user == null ? null : user.isEnabled();
  }

  private record Capture(EventEnvelope.Description description, ResolvedPublicationPolicy policy) {}

  private void persist(java.util.function.Supplier<Capture> event) {
    try {
      Capture captured = event.get();
      if (captured == null) {
        return;
      }
      if (!session.getTransactionManager().isActive()) {
        throw new IllegalStateException("An active Keycloak transaction is required");
      }
      var em = session.getProvider(JpaConnectionProvider.class).getEntityManager();
      var description = captured.description();
      // Counter locks precede the outbox insert and remain held through transaction completion.
      // Incremental callbacks retain callback order; deadlock victims roll back the whole request.
      var ordering = CaptureRepository.next(em, description.realmId(), description.userId());
      long capturedAt = CaptureRepository.databaseTime(em);
      em.persist(envelopes.serialize(description, ordering, capturedAt, captured.policy()));
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
