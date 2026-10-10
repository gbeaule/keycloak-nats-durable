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

  record Capture(
      EventEnvelope.Description description,
      ResolvedPublicationPolicy policy,
      EventEnvelope envelopes) {
    String orderingKey() {
      return description.userId() == null
          ? ""
          : EventOrdering.keyFor(description.realmId(), description.userId());
    }
  }

  static void persistBatch(EntityManager em, List<Capture> pending) {
    // Keycloak account mutations must acquire their locks before capture counters. Native JPA
    // queries can auto-flush; acquiring a JDBC counter first inverted this order for PUT/DELETE.
    em.flush();
    // Compute grouping keys once; ordered groups preserve callback order within each user and give
    // multi-user transactions one counter-lock order. This runs only during transaction prepare.
    var groups =
        pending.stream()
            .collect(
                Collectors.groupingBy(Capture::orderingKey, TreeMap::new, Collectors.toList()));
    var positions = new TreeMap<String, EventOrdering>();
    for (var entry : groups.entrySet()) {
      var captures = entry.getValue();
      var description = captures.getFirst().description();
      positions.put(
          entry.getKey(),
          allocateSequenceRange(em, description.realmId(), description.userId(), captures.size()));
    }
    // Read after all counter waits, so queued transactions do not capture an already-aged policy.
    long capturedAt = readDatabaseTime(em);
    var heads = new TreeMap<String, OutboxEvent>();
    for (var entry : groups.entrySet()) {
      var captures = entry.getValue();
      var first = positions.get(entry.getKey());
      int offset = 0;
      for (Capture capture : captures) {
        var description = capture.description();
        var ordering =
            first == null
                ? null
                : new EventOrdering(first.realmId(), first.userId(), first.sequence() + offset);
        offset++;
        var row =
            capture
                .envelopes()
                .serialize(
                    UUID.randomUUID().toString(),
                    description,
                    ordering,
                    capturedAt,
                    capture.policy());
        em.persist(row);
        if (ordering != null) {
          heads.putIfAbsent(entry.getKey(), row);
        }
      }
    }
    em.flush();
    heads.values().forEach(head -> OutboxHeads.append(em, head));
  }

  static EventOrdering allocateNextSequence(EntityManager em, String realmId, String userId) {
    return allocateSequenceRange(em, realmId, userId, 1);
  }

  static EventOrdering allocateSequenceRange(
      EntityManager em, String realmId, String userId, int count) {
    if (count <= 0) {
      throw new IllegalArgumentException("Capture range must be nonempty");
    }
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
            + " SET LAST_SEQUENCE = LAST_SEQUENCE + "
            + count
            + " WHERE ORDERING_KEY = ? AND REALM_ID = ? AND USER_ID = ?"
            + " AND LAST_SEQUENCE <= "
            + (Long.MAX_VALUE - count)
            + " RETURNING LAST_SEQUENCE";
    String insert =
        "INSERT INTO "
            + mapping.getTableName()
            + " (ORDERING_KEY, REALM_ID, USER_ID, LAST_SEQUENCE) VALUES (?, ?, ?, "
            + count
            + ") ON CONFLICT DO NOTHING RETURNING LAST_SEQUENCE";
    long sequence =
        session.doReturningWork(
            connection -> {
              Long last = execute(connection, update, key, realmId, userId);
              if (last == null) {
                // First captures can race on either unique index. A targeted UPSERT can fail on
                // the other constraint; DO NOTHING handles both and a fresh UPDATE sees the winner.
                last = execute(connection, insert, key, realmId, userId);
                if (last == null) {
                  last = execute(connection, update, key, realmId, userId);
                }
              }
              if (last == null) {
                throw new IllegalStateException("User capture sequence exhausted or mismatched");
              }
              return last;
            });
    return new EventOrdering(realmId, userId, sequence - count + 1);
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
