package io.github.keycloaknats;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity(name = "NatsOutboxEvent")
@Table(name = "KC_NATS_OUTBOX")
public class OutboxEvent {
  @Id
  @Column(name = "ID", length = 36, nullable = false)
  private String id;

  @Column(name = "SUBJECT", length = 512, nullable = false)
  private String subject;

  @Column(name = "PAYLOAD", columnDefinition = "text", nullable = false)
  private String payload;

  @Column(name = "CREATED_AT", nullable = false)
  private long createdAt;

  @Column(name = "NEXT_ATTEMPT_AT", nullable = false)
  private long nextAttemptAt;

  @Column(name = "ATTEMPTS", nullable = false)
  private long attempts;

  @Column(name = "LAST_ERROR", length = 128)
  private String lastError;

  protected OutboxEvent() {}

  public OutboxEvent(String id, String subject, String payload, long createdAt) {
    this.id = id;
    this.subject = subject;
    this.payload = payload;
    this.createdAt = createdAt;
    this.nextAttemptAt = createdAt;
  }

  public String id() {
    return id;
  }

  public String subject() {
    return subject;
  }

  public String payload() {
    return payload;
  }

  public long createdAt() {
    return createdAt;
  }

  public long attempts() {
    return attempts;
  }

  public long nextAttemptAt() {
    return nextAttemptAt;
  }

  public String lastError() {
    return lastError;
  }

  public void failed(long next, String category) {
    if (attempts < Long.MAX_VALUE) attempts++;
    nextAttemptAt = next;
    // A controlled category, never a remote error message or credential-bearing exception text.
    lastError = category;
  }
}
