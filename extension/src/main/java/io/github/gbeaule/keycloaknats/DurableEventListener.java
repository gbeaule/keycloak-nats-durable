package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
  private final List<Capture> pending = new ArrayList<>();
  private boolean enlisted;
  private boolean prepared;

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
                .map(policy -> new Capture(envelopes.describe(event), policy))
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

  private record Capture(EventEnvelope.Description description, ResolvedPublicationPolicy policy) {
    String orderingKey() {
      return description.userId() == null
          ? ""
          : new EventOrdering(description.realmId(), description.userId(), 1).key();
    }
  }

  private void stage(java.util.function.Supplier<Capture> event) {
    try {
      Capture captured = event.get();
      if (captured == null) {
        return;
      }
      if (!session.getTransactionManager().isActive()) {
        throw new IllegalStateException("An active Keycloak transaction is required");
      }
      if (prepared) {
        throw new IllegalStateException("Event capture has already prepared for commit");
      }
      enlistCapture();
      pending.add(captured);
    } catch (RuntimeException | Error failure) {
      // Keycloak catches listener failures. Throwing alone would silently commit the account
      // change.
      session.getTransactionManager().setRollbackOnly();
      throw failure;
    }
  }

  private void enlistCapture() {
    if (enlisted) {
      return;
    }
    // Initialize the provider while enlistment is still open. Prepare runs before JPA/JTA commit.
    var em = session.getProvider(JpaConnectionProvider.class).getEntityManager();
    session
        .getTransactionManager()
        .enlistPrepare(
            new AbstractKeycloakTransaction() {
              @Override
              protected void commitImpl() {
                persistPending(em);
              }

              @Override
              protected void rollbackImpl() {
                pending.clear();
              }
            });
    session
        .getTransactionManager()
        .enlistAfterCompletion(
            new AbstractKeycloakTransaction() {
              @Override
              protected void commitImpl() {
                reset();
                wakeRelay.run();
              }

              @Override
              protected void rollbackImpl() {
                reset();
              }
            });
    enlisted = true;
  }

  private void persistPending(EntityManager em) {
    prepared = true;
    try {
      // Stable sorting retains each user's callback order and gives multi-user transactions
      // one lock order. Nested error transactions finish before the caller acquires these locks.
      for (Capture capture :
          pending.stream().sorted(Comparator.comparing(Capture::orderingKey)).toList()) {
        var description = capture.description();
        var ordering = CaptureRepository.next(em, description.realmId(), description.userId());
        long capturedAt = CaptureRepository.databaseTime(em);
        em.persist(envelopes.serialize(description, ordering, capturedAt, capture.policy()));
      }
      em.flush();
      for (String key :
          pending.stream()
              .map(Capture::orderingKey)
              .filter(key -> !key.isEmpty())
              .distinct()
              .sorted()
              .toList()) {
        OutboxHeads.refresh(em, key);
      }
    } catch (RuntimeException failure) {
      session.getTransactionManager().setRollbackOnly();
      throw failure;
    } catch (Error failure) {
      session.getTransactionManager().setRollbackOnly();
      // Keycloak's prepare loop rolls back on RuntimeException, not Error.
      throw new IllegalStateException("Event capture preparation failed", failure);
    }
  }

  private void reset() {
    pending.clear();
    enlisted = false;
    prepared = false;
  }

  @Override
  public void close() {}
}
