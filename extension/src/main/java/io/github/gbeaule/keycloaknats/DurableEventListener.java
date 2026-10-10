package io.github.gbeaule.keycloaknats;

import io.github.gbeaule.keycloaknats.CaptureRepository.Capture;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;

/** Captures an allowlisted event in the same database transaction as the Keycloak operation. */
public final class DurableEventListener implements EventListenerProvider {
  private final KeycloakSession session;
  private final EventEnvelope envelopes;
  private final Runnable wakeRelay;
  private final java.util.function.Supplier<EventFilter> filter;
  private CaptureTransaction capture;

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
    stage(
        () ->
            filter
                .get()
                .resolve(event, envelopes.userSubject(event))
                .map(policy -> new Capture(envelopes.describe(event), policy, envelopes))
                .orElse(null));
  }

  @Override
  public void onEvent(AdminEvent event, boolean includeRepresentation) {
    stage(
        () -> {
          EventFilter policy = filter.get();
          String subject = envelopes.adminSubject(event);
          if (!policy.mayAccept(event, subject)) {
            return null;
          }
          String clientId = ActingClient.resolve(event, session);
          if (!policy.mayAccept(event, clientId, subject)) {
            return null;
          }
          String userId = AffectedUser.resolve(event, session);
          Boolean enabled = loadObservedEnabledState(event, userId);
          return policy
              .resolve(event, userId, enabled, clientId, subject)
              .map(
                  resolved ->
                      new Capture(
                          envelopes.describe(event, userId, enabled, clientId),
                          resolved,
                          envelopes))
              .orElse(null);
        });
  }

  private Boolean loadObservedEnabledState(AdminEvent event, String userId) {
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

  private void stage(java.util.function.Supplier<Capture> event) {
    try {
      Capture captured = event.get();
      if (captured == null) {
        return;
      }
      capture().add(captured);
    } catch (RuntimeException | Error failure) {
      // Keycloak catches listener failures and can explicitly commit despite its rollback flag.
      // A preparation guard must reject that commit, including failures before the first snapshot.
      capture().fail(failure);
      throw failure;
    }
  }

  private CaptureTransaction capture() {
    if (capture == null) {
      capture = CaptureTransaction.forSession(session, wakeRelay);
    }
    return capture;
  }

  @Override
  public void close() {}
}
