package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;

/**
 * Transaction-local PostgreSQL limits; capture and pooled connections retain their own settings.
 */
final class RelayDatabase {
  private RelayDatabase() {}

  static void limitStatements(EntityManager em, RelayDatabaseConfig config) {
    em.createNativeQuery(
            "select set_config('statement_timeout', :statement, true),"
                + " set_config('lock_timeout', :lock, true)",
            Object[].class)
        .setParameter("statement", Integer.toString(config.statementTimeoutMillis()))
        .setParameter("lock", Integer.toString(config.lockTimeoutMillis()))
        .getSingleResult();
  }
}
