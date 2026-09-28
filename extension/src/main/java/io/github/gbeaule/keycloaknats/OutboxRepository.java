package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.TypedQuery;
import java.util.Optional;
import org.hibernate.Timeouts;
import org.hibernate.jpa.SpecHints;

/** Uses Keycloak's managed entity manager and the caller's database transaction. */
final class OutboxRepository {
  private static final String DUE_HEAD =
      """
      select event from NatsOutboxEvent event
      where event.nextAttemptAt <= :now
        and (event.orderingKey is null or not exists (
          select 1 from NatsOutboxEvent predecessor
          where predecessor.orderingKey = event.orderingKey
            and predecessor.userSequence < event.userSequence
        ))
      """;

  private OutboxRepository() {}

  static Optional<OutboxEvent> lockNextExpired(EntityManager entityManager, long now) {
    // Expiry applies to queued successors and delayed retries, but never races a locked send.
    return lock(
        entityManager
            .createQuery(
                "select event from NatsOutboxEvent event where event.expiresAt <= :now"
                    + " order by event.expiresAt, event.id",
                OutboxEvent.class)
            .setParameter("now", now));
  }

  static void discard(
      EntityManager entityManager, OutboxEvent row, DiscardReason reason, long now) {
    entityManager.persist(new DiscardAudit(row, reason, now));
    entityManager.remove(row);
  }

  static Optional<OutboxEvent> lockNextDue(EntityManager entityManager, long now) {
    // The predecessor scan includes locked and delayed rows; only eligible heads skip locks.
    return lock(
        entityManager
            .createQuery(
                DUE_HEAD + " order by event.nextAttemptAt, event.createdAt, event.id",
                OutboxEvent.class)
            .setParameter("now", now));
  }

  static Optional<OutboxEvent> lockPrepared(
      EntityManager entityManager, String id, long version, long now) {
    return lock(
        entityManager
            .createQuery(
                DUE_HEAD
                    + " and event.id = :id and event.version = :version"
                    + " and event.publicationMayHaveOccurred = true",
                OutboxEvent.class)
            .setParameter("now", now)
            .setParameter("id", id)
            .setParameter("version", version));
  }

  private static Optional<OutboxEvent> lock(TypedQuery<OutboxEvent> query) {
    // Hibernate supplies schema qualification and PostgreSQL FOR UPDATE SKIP LOCKED.
    return query
        .setMaxResults(1)
        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
        .setHint(SpecHints.HINT_SPEC_LOCK_TIMEOUT, Timeouts.SKIP_LOCKED_MILLI)
        .getResultList()
        .stream()
        .findFirst();
  }
}
