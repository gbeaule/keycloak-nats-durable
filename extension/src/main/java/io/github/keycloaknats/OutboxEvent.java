package io.github.keycloaknats;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Durable event identity and payload, with bounded retry metadata updated under a row lock. */
@Entity(name = "NatsOutboxEvent")
@Table(name = "KC_NATS_OUTBOX")
public class OutboxEvent {
  @Id
  @Column(name = "ID", length = 36, nullable = false)
  private String id;

  @Column(name = "SUBJECT", length = 512, nullable = false, updatable = false)
  private String subject;

  @Column(name = "PAYLOAD", columnDefinition = "text", nullable = false, updatable = false)
  private String payload;

  @Column(name = "CREATED_AT", nullable = false, updatable = false)
  private long createdAt;

  @Column(name = "NEXT_ATTEMPT_AT", nullable = false)
  private long nextAttemptAt;

  @Column(name = "ATTEMPTS", nullable = false)
  private long attempts;

  @Column(name = "LAST_ERROR", length = 128)
  private String lastError;

  /** Required by JPA for hydration. */
  protected OutboxEvent() {}

  /** Creates an immediately eligible event; identity, subject and bytes never change on retry. */
  public OutboxEvent(String id, String subject, String payload, long createdAt) {
    this.id = id;
    this.subject = subject;
    this.payload = payload;
    this.createdAt = createdAt;
    this.nextAttemptAt = createdAt;
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
