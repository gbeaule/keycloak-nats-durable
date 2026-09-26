package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.hibernate.Timeouts;
import org.hibernate.jpa.SpecHints;

/** Uses Keycloak's managed entity manager and the caller's database transaction. */
final class OutboxRepository {
  private static final String NEXT_DUE_EVENT =
      """
      select event from NatsOutboxEvent event
      where event.nextAttemptAt <= :now
      order by event.nextAttemptAt, event.createdAt, event.id
      """;

  private OutboxRepository() {}

  static Optional<OutboxEvent> lockNextDue(EntityManager entityManager, long now) {
    // JPQL uses entity/field names. Hibernate generates PostgreSQL FOR UPDATE SKIP LOCKED.
    // This Hibernate sentinel skips another node's locked row instead of waiting for it.
    return entityManager
        .createQuery(NEXT_DUE_EVENT, OutboxEvent.class)
        .setParameter("now", now)
        .setMaxResults(1)
        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
        .setHint(SpecHints.HINT_SPEC_LOCK_TIMEOUT, Timeouts.SKIP_LOCKED_MILLI)
        .getResultList()
        .stream()
        .findFirst();
  }
}
