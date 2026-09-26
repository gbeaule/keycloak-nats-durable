package io.github.gbeaule.keycloaknats;

/** Carries only fixed validation messages defined by this extension, safe for operational logs. */
final class UnsafeStreamException extends IllegalStateException {
  UnsafeStreamException(String reason) {
    super(reason);
  }
}
