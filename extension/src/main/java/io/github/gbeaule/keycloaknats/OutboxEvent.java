package io.github.gbeaule.keycloaknats;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.nio.charset.StandardCharsets;

/** Durable event identity and payload, with bounded retry metadata updated under a row lock. */
@Entity(name = "NatsOutboxEvent")
@Table(name = "KC_NATS_OUTBOX")
public class OutboxEvent {
  @Id
  @Column(name = "ID", length = 36, nullable = false)
  private String id;

  @Version
  @Column(name = "VERSION", nullable = false)
  private long version;

  @Column(name = "SUBJECT", length = 512, nullable = false, updatable = false)
  private String subject;

  @Column(name = "PAYLOAD", columnDefinition = "text", nullable = false, updatable = false)
  private String payload;

  @Column(name = "REALM_ID", length = 255, nullable = false, updatable = false)
  private String realmId;

  @Column(name = "EVENT_TYPE", length = 128, nullable = false, updatable = false)
  private String eventType;

  @Column(name = "PAYLOAD_SHA256", length = 64, nullable = false, updatable = false)
  private String payloadSha256;

  @Column(name = "ORDERING_KEY", length = 2048, updatable = false)
  private String orderingKey;

  @Column(name = "USER_SEQUENCE", updatable = false)
  private Long userSequence;

  @Column(name = "MAX_AGE_SECONDS", updatable = false)
  private Integer maxAgeSeconds;

  @Column(name = "MAX_FAILURES", updatable = false)
  private Integer maxFailures;

  @Column(name = "EXPIRES_AT", updatable = false)
  private Long expiresAt;

  @Column(name = "FILTER_SHA256", length = 64, nullable = false, updatable = false)
  private String filterSha256;

  @Column(name = "RULE_ID", columnDefinition = "text", updatable = false)
  private String ruleId;

  @Column(name = "PUBLICATION_MAY_HAVE_OCCURRED", nullable = false)
  private boolean publicationMayHaveOccurred;

  @Column(name = "CREATED_AT", nullable = false, updatable = false)
  private long createdAt;

  @Column(name = "NEXT_ATTEMPT_AT", nullable = false)
  private long nextAttemptAt;

  @Column(name = "NEXT_EXPIRY_ATTEMPT_AT")
  private Long nextExpiryAttemptAt;

  @Column(name = "ATTEMPTS", nullable = false)
  private long attempts;

  @Column(name = "LAST_ERROR", length = 128)
  private String lastError;

  /** Required by JPA for hydration. */
  protected OutboxEvent() {}

  /** Creates an immediately eligible event; identity, subject and bytes never change on retry. */
  OutboxEvent(
      String id,
      String subject,
      String payload,
      long createdAt,
      String realmId,
      String eventType,
      EventOrdering ordering,
      ResolvedPublicationPolicy policy) {
    this.id = id;
    this.subject = subject;
    this.payload = payload;
    this.createdAt = createdAt;
    this.nextAttemptAt = createdAt;
    if (realmId == null || realmId.isBlank() || realmId.codePointCount(0, realmId.length()) > 255) {
      throw new IllegalArgumentException("Event realm exceeds storage limit or is missing");
    }
    this.realmId = realmId;
    this.eventType = eventType;
    this.payloadSha256 = EventFilter.digest(payload.getBytes(StandardCharsets.UTF_8));
    this.orderingKey = ordering == null ? null : ordering.key();
    this.userSequence = ordering == null ? null : ordering.sequence();
    this.maxAgeSeconds = policy.policy().maxAgeSeconds();
    this.maxFailures = policy.policy().maxFailures();
    this.expiresAt =
        maxAgeSeconds == null ? null : Math.addExact(createdAt, maxAgeSeconds.longValue() * 1000);
    this.nextExpiryAttemptAt = expiresAt;
    this.filterSha256 = policy.filterSha256();
    this.ruleId = policy.ruleId();
  }

  String realmId() {
    return realmId;
  }

  String eventType() {
    return eventType;
  }

  String payloadSha256() {
    return payloadSha256;
  }

  String orderingKey() {
    return orderingKey;
  }

  Long userSequence() {
    return userSequence;
  }

  Long expiresAt() {
    return expiresAt;
  }

  ResolvedPublicationPolicy publicationPolicy() {
    return new ResolvedPublicationPolicy(
        new PublicationPolicy(maxAgeSeconds, maxFailures), filterSha256, ruleId);
  }

  boolean publicationMayHaveOccurred() {
    return publicationMayHaveOccurred;
  }

  void markPublicationIntent() {
    publicationMayHaveOccurred = true;
  }

  long version() {
    return version;
  }

  Long nextExpiryAttemptAt() {
    return nextExpiryAttemptAt;
  }

  void deferResolution(long next) {
    // Move behind other ready work even if the cooldown elapses before the next worker scan.
    nextAttemptAt = next;
    nextExpiryAttemptAt = expiresAt == null ? null : Math.max(expiresAt, next);
  }

  /** Returns the persisted deduplication identity shared by the envelope and NATS header. */
  public String id() {
    return id;
  }

  /** Returns the original routing subject, including the realm token. */
  public String subject() {
    return subject;
  }

  /** Returns the original serialized CloudEvent, reused byte-for-byte on retry. */
  public String payload() {
    return payload;
  }

  /** Returns capture time in epoch milliseconds, not a commit-order sequence. */
  public long createdAt() {
    return createdAt;
  }

  /** Returns failed attempts, saturating at {@link Long#MAX_VALUE}. */
  public long attempts() {
    return attempts;
  }

  /** Returns the earliest retry time in epoch milliseconds. */
  public long nextAttemptAt() {
    return nextAttemptAt;
  }

  /** Returns a safe exception category, never the remote error text. */
  public String lastError() {
    return lastError;
  }

  /** Schedules another retry even after the attempt counter reaches its storage limit. */
  public void failed(long next, String category) {
    if (attempts < Long.MAX_VALUE) {
      attempts++;
    }
    nextAttemptAt = next;
    // A controlled category, never a remote error message or credential-bearing exception text.
    lastError = category;
  }
}
