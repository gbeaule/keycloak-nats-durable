package io.github.gbeaule.keycloaknats;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Read-only mapping; repositories explicitly write sequence allocation and pending-head progress.
 */
@Entity(name = "NatsCaptureCounter")
@Table(name = "KC_NATS_CAPTURE_COUNTER")
public class CaptureCounter {
  @Id
  @Column(name = "ORDERING_KEY", length = 2048, nullable = false)
  private String orderingKey;

  @Column(name = "REALM_ID", length = 255, nullable = false, updatable = false)
  private String realmId;

  @Column(name = "USER_ID", length = 255, nullable = false, updatable = false)
  private String userId;

  @Column(name = "LAST_SEQUENCE", nullable = false, updatable = false)
  private long lastSequence;

  @Column(name = "HEAD_EVENT_ID", length = 36, updatable = false)
  private String headEventId;

  @Column(name = "HEAD_NEXT_ATTEMPT_AT", updatable = false)
  private Long headNextAttemptAt;

  @Column(name = "HEAD_CREATED_AT", updatable = false)
  private Long headCreatedAt;

  /** Required by JPA for hydration. */
  protected CaptureCounter() {}
}
