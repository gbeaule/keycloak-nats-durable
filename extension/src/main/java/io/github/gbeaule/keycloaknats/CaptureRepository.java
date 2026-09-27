package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
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
    String sql =
        "INSERT INTO "
            + mapping.getTableName()
            + " AS counter"
            + " (ORDERING_KEY, REALM_ID, USER_ID, LAST_SEQUENCE) VALUES (?, ?, ?, 1)"
            + " ON CONFLICT (REALM_ID, USER_ID) DO UPDATE"
            + " SET LAST_SEQUENCE = counter.LAST_SEQUENCE + 1"
            + " WHERE counter.LAST_SEQUENCE < "
            + Long.MAX_VALUE
            + " RETURNING LAST_SEQUENCE";
    long sequence =
        session.doReturningWork(
            connection -> {
              try (var statement = connection.prepareStatement(sql)) {
                statement.setString(1, identity.key());
                statement.setString(2, realmId);
                statement.setString(3, userId);
                try (var result = statement.executeQuery()) {
                  if (!result.next()) {
                    throw new IllegalStateException("User capture sequence exhausted");
                  }
                  return result.getLong(1);
                }
              }
            });
    return new EventOrdering(realmId, userId, sequence);
  }

  static long databaseTime(EntityManager em) {
    return ((Number)
            em.createNativeQuery(
                    "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)", Long.class)
                .getSingleResult())
        .longValue();
  }
}
