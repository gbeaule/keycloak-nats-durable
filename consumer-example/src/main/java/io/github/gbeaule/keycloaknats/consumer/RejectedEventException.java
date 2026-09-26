package io.github.gbeaule.keycloaknats.consumer;

/** A deterministic rejection eligible for an explicitly configured quarantine or discard policy. */
public final class RejectedEventException extends IllegalArgumentException {
  /** Fixed diagnostic codes keep payloads and parser exception text out of operational logs. */
  public enum Reason {
    INVALID_JSON,
    INVALID_EVENT_ID,
    UNSUPPORTED_ENVELOPE,
    OVERSIZE,
    HANDLER_REJECTED
  }

  private final Reason reason;

  /** Application handlers can deliberately reject an event with {@code HANDLER_REJECTED}. */
  public RejectedEventException(Reason reason) {
    super(reason.name());
    this.reason = reason;
  }

  /** Returns a safe diagnostic code, never event content. */
  public Reason reason() {
    return reason;
  }
}
