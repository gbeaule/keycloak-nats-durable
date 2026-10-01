package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.util.Objects;

/** Maintains the indexed pending head in the same transaction as an outbox change. */
final class OutboxHeads {
  private OutboxHeads() {}

  static void refresh(EntityManager em, String orderingKey) {
    em.flush();
    if (orderingKey == null) {
      return;
    }
    // Lock before the head query: a fresh READ COMMITTED snapshot must see a capture or
    // resolution that committed while we waited. The query never locks a successor's row.
    Objects.requireNonNull(
        em.find(CaptureCounter.class, orderingKey, LockModeType.PESSIMISTIC_WRITE),
        "Pending event has no capture counter");
    var head =
        em
            .createQuery(
                "from NatsOutboxEvent where orderingKey = :key order by userSequence",
                OutboxEvent.class)
            .setParameter("key", orderingKey)
            .setMaxResults(1)
            .getResultList()
            .stream()
            .findFirst()
            .orElse(null);
    // Explicitly write only the projection. Managed counter snapshots never write back stale
    // sequence values or heads through dirty checking.
    em.createQuery(
            "update NatsCaptureCounter set headEventId = :id, headNextAttemptAt = :due,"
                + " headCreatedAt = :created where orderingKey = :key")
        .setParameter("id", head == null ? null : head.id())
        .setParameter("due", head == null ? null : head.nextAttemptAt())
        .setParameter("created", head == null ? null : head.createdAt())
        .setParameter("key", orderingKey)
        .executeUpdate();
  }
}
