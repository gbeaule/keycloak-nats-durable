package io.github.gbeaule.keycloaknats.consumer;

/** A timed-out handler did not stop; stop this worker instead of accumulating abandoned tasks. */
public final class UnresponsiveHandlerException extends IllegalStateException {
  UnresponsiveHandlerException() {
    super("Processing did not stop within the cleanup deadline; restart the worker");
  }
}
