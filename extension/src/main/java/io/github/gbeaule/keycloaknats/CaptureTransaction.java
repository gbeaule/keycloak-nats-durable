package io.github.gbeaule.keycloaknats;

import io.github.gbeaule.keycloaknats.CaptureRepository.Capture;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.engine.spi.SessionImplementor;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSession;

/** One capture buffer and commit guard shared by every listener in a Keycloak session. */
final class CaptureTransaction {
  private static final String SESSION_ATTRIBUTE = CaptureTransaction.class.getName();
  private final KeycloakSession session;
  private final Runnable wakeRelay;
  private final List<Capture> pending = new ArrayList<>();
  private EntityManager em;
  private Throwable failure;
  private boolean enlisted;
  private boolean prepared;

  private CaptureTransaction(KeycloakSession session, Runnable wakeRelay) {
    this.session = session;
    this.wakeRelay = wakeRelay;
  }

  static CaptureTransaction forSession(KeycloakSession session, Runnable wakeRelay) {
    var capture = (CaptureTransaction) session.getAttribute(SESSION_ATTRIBUTE);
    if (capture == null) {
      capture = new CaptureTransaction(session, wakeRelay);
      session.setAttribute(SESSION_ATTRIBUTE, capture);
    }
    return capture;
  }

  void add(Capture capture) {
    if (!session.getTransactionManager().isActive()) {
      throw new IllegalStateException("An active Keycloak transaction is required");
    }
    if (prepared) {
      throw new IllegalStateException("Event capture has already prepared for commit");
    }
    enlist();
    checkFailure();
    if (em == null) {
      // Guard first: provider initialization can fail before Keycloak enlists its database work.
      em = session.getProvider(JpaConnectionProvider.class).getEntityManager();
    }
    pending.add(capture);
  }

  void fail(Throwable cause) {
    var tx = session.getTransactionManager();
    if (!tx.isActive()) {
      return;
    }
    tx.setRollbackOnly();
    if (failure == null) {
      failure = cause;
    }
    if (prepared && em != null) {
      markDatabaseRollbackOnly(cause);
    }
    if (!enlisted) {
      try {
        enlist();
      } catch (RuntimeException | Error guardFailure) {
        if (guardFailure != cause) {
          cause.addSuppressed(guardFailure);
        }
        // A first callback can arrive after Keycloak has closed preparation registration.
        markDatabaseRollbackOnly(cause);
      }
    }
  }

  private void markDatabaseRollbackOnly(Throwable cause) {
    try {
      if (em == null) {
        em = session.getProvider(JpaConnectionProvider.class).getEntityManager();
      }
      // Mark the owning JDBC/JTA transaction, bypassing JPA's public JTA-access restriction.
      em.unwrap(SessionImplementor.class).accessTransaction().markRollbackOnly();
    } catch (RuntimeException | Error rollbackFailure) {
      if (rollbackFailure != cause) {
        cause.addSuppressed(rollbackFailure);
      }
    }
  }

  private void enlist() {
    if (enlisted) {
      return;
    }
    session
        .getTransactionManager()
        .enlistPrepare(
            new AbstractKeycloakTransaction() {
              @Override
              protected void commitImpl() {
                prepare();
              }

              @Override
              protected void rollbackImpl() {
                // Also clear state if completion-hook registration failed after this guard
                // enlisted.
                reset();
              }
            });
    enlisted = true;
    session
        .getTransactionManager()
        .enlistAfterCompletion(
            new AbstractKeycloakTransaction() {
              @Override
              protected void commitImpl() {
                try {
                  checkFailure();
                } finally {
                  reset();
                }
                wakeRelay.run();
              }

              @Override
              protected void rollbackImpl() {
                reset();
              }
            });
  }

  private void prepare() {
    prepared = true;
    try {
      checkFailure();
      CaptureRepository.persistBatch(em, pending);
      // A callback emitted by flush must not be hidden by Keycloak swallowing its exception.
      checkFailure();
    } catch (RuntimeException cause) {
      session.getTransactionManager().setRollbackOnly();
      throw cause;
    } catch (Error cause) {
      session.getTransactionManager().setRollbackOnly();
      // Keycloak's prepare loop rolls back on RuntimeException, not Error.
      throw new IllegalStateException("Event capture preparation failed", cause);
    }
  }

  private void checkFailure() {
    if (failure != null) {
      throw new IllegalStateException("Event capture failed before commit", failure);
    }
  }

  private void reset() {
    pending.clear();
    em = null;
    failure = null;
    enlisted = false;
    prepared = false;
  }
}
