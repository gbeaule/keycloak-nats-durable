package io.github.gbeaule.keycloaknats;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Survives user deletion and outbox draining; only capture transactions lock this row. */
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

  @Column(name = "LAST_SEQUENCE", nullable = false)
  private long lastSequence;

  /** Required by JPA for hydration. */
  protected CaptureCounter() {}
}
