package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.KeycloakSessionTask;

/** Confirms commit after the Keycloak runner has completed and closed its managed session. */
final class KeycloakTransactions implements Transactions {
  private final Consumer<KeycloakSessionTask> runner;

  KeycloakTransactions(Consumer<KeycloakSessionTask> runner) {
    this.runner = runner;
  }

  @Override
  public void run(Consumer<EntityManager> work) {
    var committed = new AtomicBoolean();
    runner.accept(
        session -> {
          session
              .getTransactionManager()
              .enlistAfterCompletion(
                  new AbstractKeycloakTransaction() {
                    @Override
                    protected void commitImpl() {
                      committed.set(true);
                    }

                    @Override
                    protected void rollbackImpl() {}
                  });
          work.accept(session.getProvider(JpaConnectionProvider.class).getEntityManager());
        });
    // Session close can silently roll back a rollback-only transaction. Checking before close
    // would race that decision; only a completed commit confirms the result returned to a worker.
    if (!committed.get()) {
      throw new IllegalStateException("Keycloak transaction commit was not confirmed");
    }
  }
}
