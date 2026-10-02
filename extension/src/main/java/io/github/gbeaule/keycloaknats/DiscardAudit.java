package io.github.gbeaule.keycloaknats;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Local metadata about an abandoned event; contains no replayable payload or remote error text. */
@Entity(name = "NatsDiscardAudit")
@Table(name = "KC_NATS_DISCARD_AUDIT")
public class DiscardAudit {
  @Id
  @Column(name = "ID", length = 36, nullable = false)
  private String id;

  @Column(name = "REALM_ID", length = 255, nullable = false)
  private String realmId;

  @Column(name = "EVENT_TYPE", length = 128, nullable = false)
  private String eventType;

  @Column(name = "SUBJECT", length = 512, nullable = false)
  private String subject;

  @Column(name = "PAYLOAD_SHA256", length = 64, nullable = false)
  private String payloadSha256;

  @Column(name = "ORDERING_KEY", length = 2048)
  private String orderingKey;

  @Column(name = "USER_SEQUENCE")
  private Long userSequence;

  @Column(name = "MAX_AGE_SECONDS")
  private Integer maxAgeSeconds;

  @Column(name = "MAX_FAILURES")
  private Integer maxFailures;

  @Column(name = "EXPIRES_AT")
  private Long expiresAt;

  @Column(name = "FILTER_SHA256", length = 64, nullable = false)
  private String filterSha256;

  @Column(name = "RULE_ID", columnDefinition = "text")
  private String ruleId;

  @Column(name = "CREATED_AT", nullable = false)
  private long createdAt;

  @Column(name = "DISCARDED_AT", nullable = false)
  private long discardedAt;

  @Enumerated(EnumType.STRING)
  @Column(name = "REASON", length = 32, nullable = false)
  private DiscardReason reason;

  @Column(name = "ATTEMPTS", nullable = false)
  private long attempts;

  @Column(name = "PUBLICATION_MAY_HAVE_OCCURRED", nullable = false)
  private boolean publicationMayHaveOccurred;

  /** Required by JPA for hydration. */
  protected DiscardAudit() {}

  DiscardAudit(OutboxEvent row, DiscardReason reason, long discardedAt) {
    this.id = row.id();
    this.realmId = row.realmId();
    this.eventType = row.eventType();
    this.subject = row.subject();
    this.payloadSha256 = row.payloadSha256();
    this.orderingKey = row.orderingKey();
    this.userSequence = row.userSequence();
    var policy = row.publicationPolicy();
    this.maxAgeSeconds = policy.policy().maxAgeSeconds();
    this.maxFailures = policy.policy().maxFailures();
    this.expiresAt = row.expiresAt();
    this.filterSha256 = policy.filterSha256();
    this.ruleId = policy.ruleId();
    this.createdAt = row.createdAt();
    this.discardedAt = discardedAt;
    this.reason = reason;
    this.attempts = row.attempts();
    this.publicationMayHaveOccurred = row.publicationMayHaveOccurred();
  }
}
