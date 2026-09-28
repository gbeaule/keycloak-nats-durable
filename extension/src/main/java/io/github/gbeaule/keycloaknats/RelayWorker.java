package io.github.gbeaule.keycloaknats;

/**
 * Prompt local wakeups, immediate backlog draining and bounded idle scans for each relay worker.
 */
final class RelayWorker implements Runnable {
  private final OutboxRelay relay;
  private final RelayWakeup wakeup;
  private final BridgeConfig config;

  RelayWorker(OutboxRelay relay, RelayWakeup wakeup, BridgeConfig config) {
    this.relay = relay;
    this.wakeup = wakeup;
    this.config = config;
  }

  @Override
  public void run() {
    long delay = config.pollInterval().toMillis();
    try {
      while (wakeup.isOpen() && !Thread.currentThread().isInterrupted()) {
        var result = relay.runBatch();
        if (result.outcome() == OutboxRelay.Outcome.STOPPED) {
          return;
        }
        if (result.processed() == config.batchSize()
            && result.outcome() != OutboxRelay.Outcome.TRANSACTION_FAILED) {
          delay = 0;
        } else if (result.processed() > 0) {
          delay = config.pollInterval().toMillis();
        } else {
          delay =
              Math.min(
                  config.idlePollMax().toMillis(),
                  Math.max(config.pollInterval().toMillis(), delay * 2));
        }
        if (!wakeup.awaitSignal(delay)) {
          return;
        }
      }
    } catch (InterruptedException shutdown) {
      Thread.currentThread().interrupt();
    }
  }
}
