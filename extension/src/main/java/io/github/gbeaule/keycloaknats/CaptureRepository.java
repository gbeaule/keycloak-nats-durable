package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import java.sql.Connection;
import java.sql.SQLException;
import org.hibernate.Session;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;

/**
 * Allocates positions on Keycloak's managed connection, without a separate commit or datasource.
 */
final class CaptureRepository {
  private CaptureRepository() {}

  static EventOrdering next(EntityManager em, String realmId, String userId) {
    if (userId == null) {
      return null;
    }
    var identity = new EventOrdering(realmId, userId, 1);
    var session = em.unwrap(Session.class);
    var factory = session.getSessionFactory().unwrap(SessionFactoryImplementor.class);
    var mapping =
        (AbstractEntityPersister)
            factory.getMappingMetamodel().getEntityDescriptor(CaptureCounter.class);
    // Hibernate supplies the qualified/quoted table name, including a configured custom schema.
    String update =
        "UPDATE "
            + mapping.getTableName()
            + " SET LAST_SEQUENCE = LAST_SEQUENCE + 1"
            + " WHERE ORDERING_KEY = ? AND REALM_ID = ? AND USER_ID = ?"
            + " AND LAST_SEQUENCE < "
            + Long.MAX_VALUE
            + " RETURNING LAST_SEQUENCE";
    String insert =
        "INSERT INTO "
            + mapping.getTableName()
            + " (ORDERING_KEY, REALM_ID, USER_ID, LAST_SEQUENCE) VALUES (?, ?, ?, 1)"
            + " ON CONFLICT DO NOTHING RETURNING LAST_SEQUENCE";
    long sequence =
        session.doReturningWork(
            connection -> {
              Long next = execute(connection, update, identity);
              if (next == null) {
                // Concurrent first captures can conflict on either unique index. Handle both,
                // then use a fresh statement snapshot to see the winner's committed counter.
                next = execute(connection, insert, identity);
                if (next == null) {
                  next = execute(connection, update, identity);
                }
              }
              if (next == null) {
                throw new IllegalStateException("User capture sequence exhausted");
              }
              return next;
            });
    return new EventOrdering(realmId, userId, sequence);
  }

  private static Long execute(Connection connection, String sql, EventOrdering identity)
      throws SQLException {
    try (var statement = connection.prepareStatement(sql)) {
      statement.setString(1, identity.key());
      statement.setString(2, identity.realmId());
      statement.setString(3, identity.userId());
      try (var result = statement.executeQuery()) {
        return result.next() ? result.getLong(1) : null;
      }
    }
  }

  static long databaseTime(EntityManager em) {
    return ((Number)
            em.createNativeQuery(
                    "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)", Long.class)
                .getSingleResult())
        .longValue();
  }
}
