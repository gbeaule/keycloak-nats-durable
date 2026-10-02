package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.time.Duration;
import java.util.List;
import org.hibernate.Timeouts;
import org.hibernate.jpa.SpecHints;

/** Audit-only queries on the caller's managed connection and transaction. */
public final class AuditRepository {
  private AuditRepository() {}

  /** A database snapshot; oldest eligible age measures time since discard, in seconds. */
  public record Statistics(long retainedRows, double oldestEligibleAgeSeconds, long databaseTime) {}

  static List<DiscardAudit> lockExpired(EntityManager em, long cutoff, int limit) {
    // Hibernate supplies the configured schema and PostgreSQL FOR UPDATE SKIP LOCKED.
    return em.createQuery(
            "select audit from NatsDiscardAudit audit where audit.discardedAt <= :cutoff"
                + " order by audit.discardedAt, audit.id",
            DiscardAudit.class)
        .setParameter("cutoff", cutoff)
        .setMaxResults(limit)
        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
        .setHint(SpecHints.HINT_SPEC_LOCK_TIMEOUT, Timeouts.SKIP_LOCKED_MILLI)
        .getResultList();
  }

  /** Reads aggregate metadata using database time; the caller bounds the transaction. */
  public static Statistics statistics(EntityManager em, Duration retention) {
    long now = CaptureRepository.readDatabaseTime(em);
    var result =
        em.createQuery(
                "select count(audit), min(case when audit.discardedAt <= :cutoff"
                    + " then audit.discardedAt else null end) from NatsDiscardAudit audit",
                Object[].class)
            .setParameter("cutoff", now - retention.toMillis())
            .getSingleResult();
    Long oldest = (Long) result[1];
    return new Statistics((Long) result[0], oldest == null ? 0 : (now - oldest) / 1000.0, now);
  }

  static void limitStatements(EntityManager em, int timeoutSeconds) {
    em.createNativeQuery(
            "select set_config('statement_timeout', :timeout, true),"
                + " set_config('lock_timeout', :timeout, true)",
            Object[].class)
        .setParameter("timeout", Integer.toString(timeoutSeconds * 1000))
        .getSingleResult();
  }
}
