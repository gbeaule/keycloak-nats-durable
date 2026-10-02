package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.TypedQuery;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;
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

  static DiscardReason recordFailureAndScheduleRetry(
      EntityManager em, OutboxEvent row, long now, long retryAt, String category) {
    row.recordPublicationFailure(retryAt, category);
    DiscardReason reason = discardIfEligible(em, row, now);
    if (reason == null) {
      OutboxHeads.refresh(em, row.orderingKey());
    }
    return reason;
  }

  static void deferResolution(EntityManager em, OutboxEvent row, long retryAt) {
    row.deferResolution(retryAt);
    OutboxHeads.refresh(em, row.orderingKey());
  }

  static DiscardReason discardIfEligible(EntityManager em, OutboxEvent row, long now) {
    var reason =
        row.publicationPolicy().policy().discardReason(row.createdAt(), now, row.attempts());
    if (reason != null) {
      discard(em, row, reason, now);
    }
    return reason;
  }

  static Optional<OutboxEvent> lockNextExpired(EntityManager entityManager, long now) {
    // Expiry applies to queued successors and delayed retries, but never races a locked send.
    return lock(
        entityManager
            .createQuery(
                "select event from NatsOutboxEvent event where event.nextExpiryAttemptAt <= :now"
                    + " order by event.nextExpiryAttemptAt, event.id",
                OutboxEvent.class)
            .setParameter("now", now));
  }

  static void discard(
      EntityManager entityManager, OutboxEvent row, DiscardReason reason, long now) {
    entityManager.persist(new DiscardAudit(row, reason, now));
    remove(entityManager, row);
  }

  static void remove(EntityManager entityManager, OutboxEvent row) {
    entityManager.remove(row);
    OutboxHeads.refresh(entityManager, row.orderingKey());
  }

  static Optional<OutboxEvent> lockNextDue(EntityManager entityManager, long now) {
    // Each indexed scan claims at most one row. Both locks end with the short preparation
    // transaction; only the chosen event is reacquired for the subsequent broker request.
    var ordered =
        lock(
            entityManager
                .createQuery(
                    "select event from NatsCaptureCounter counter"
                        + " join NatsOutboxEvent event on event.id = counter.headEventId"
                        + " where counter.headEventId is not null"
                        + " and counter.headNextAttemptAt <= :now"
                        + " order by counter.headNextAttemptAt,"
                        + " counter.headCreatedAt, counter.headEventId",
                    OutboxEvent.class)
                .setParameter("now", now));
    var independent =
        lock(
            entityManager
                .createQuery(
                    "select event from NatsOutboxEvent event"
                        + " where event.orderingKey is null and event.nextAttemptAt <= :now"
                        + " order by event.nextAttemptAt, event.createdAt, event.id",
                    OutboxEvent.class)
                .setParameter("now", now));
    return Stream.concat(ordered.stream(), independent.stream())
        .min(
            Comparator.comparingLong(OutboxEvent::nextAttemptAt)
                .thenComparingLong(OutboxEvent::createdAt)
                .thenComparing(OutboxEvent::id));
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

  static Optional<OutboxEvent> lockUnchanged(EntityManager entityManager, String id, long version) {
    return lock(
        entityManager
            .createQuery(
                "select event from NatsOutboxEvent event"
                    + " where event.id = :id and event.version = :version",
                OutboxEvent.class)
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
