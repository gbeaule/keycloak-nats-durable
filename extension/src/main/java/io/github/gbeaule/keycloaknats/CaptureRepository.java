package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;

/**
 * Allocates positions on Keycloak's managed connection, without a separate commit or datasource.
 */
final class CaptureRepository {
  private CaptureRepository() {}

  record Capture(EventEnvelope.Description description, ResolvedPublicationPolicy policy) {
    String orderingKey() {
      return description.userId() == null
          ? ""
          : EventOrdering.keyFor(description.realmId(), description.userId());
    }
  }

  static void persistBatch(EntityManager em, EventEnvelope envelopes, List<Capture> pending) {
    // Compute grouping keys once; ordered groups preserve callback order within each user and give
    // multi-user transactions one counter-lock order. This runs only during transaction prepare.
    var groups =
        pending.stream()
            .collect(
                Collectors.groupingBy(Capture::orderingKey, TreeMap::new, Collectors.toList()));
    for (var captures : groups.values()) {
      for (Capture capture : captures) {
        var description = capture.description();
        var ordering = allocateNextSequence(em, description.realmId(), description.userId());
        long capturedAt = readDatabaseTime(em);
        em.persist(
            envelopes.serialize(
                UUID.randomUUID().toString(), description, ordering, capturedAt, capture.policy()));
      }
    }
    em.flush();
    groups.keySet().stream()
        .filter(key -> !key.isEmpty())
        .forEach(key -> OutboxHeads.refresh(em, key));
  }

  static EventOrdering allocateNextSequence(EntityManager em, String realmId, String userId) {
    if (userId == null) {
      return null;
    }
    String key = EventOrdering.keyFor(realmId, userId);
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
              Long next = execute(connection, update, key, realmId, userId);
              if (next == null) {
                // Concurrent first captures can conflict on either unique index. Handle both,
                // then use a fresh statement snapshot to see the winner's committed counter.
                next = execute(connection, insert, key, realmId, userId);
                if (next == null) {
                  next = execute(connection, update, key, realmId, userId);
                }
              }
              if (next == null) {
                throw new IllegalStateException("User capture sequence exhausted");
              }
              return next;
            });
    return new EventOrdering(realmId, userId, sequence);
  }

  private static Long execute(
      Connection connection, String sql, String key, String realmId, String userId)
      throws SQLException {
    try (var statement = connection.prepareStatement(sql)) {
      statement.setString(1, key);
      statement.setString(2, realmId);
      statement.setString(3, userId);
      try (var result = statement.executeQuery()) {
        return result.next() ? result.getLong(1) : null;
      }
    }
  }

  static long readDatabaseTime(EntityManager em) {
    return ((Number)
            em.createNativeQuery(
                    "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)", Long.class)
                .getSingleResult())
        .longValue();
  }
}
