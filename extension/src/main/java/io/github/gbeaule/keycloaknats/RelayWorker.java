package io.github.gbeaule.keycloaknats;

/** One worker per node: prompt local wakeups, immediate backlog draining and bounded idle scans. */
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
        int published = relay.runBatch();
        if (published == config.batchSize()) {
          delay = 0;
        } else if (published > 0) {
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
