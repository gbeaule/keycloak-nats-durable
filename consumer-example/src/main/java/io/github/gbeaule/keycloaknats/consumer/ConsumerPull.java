package io.github.gbeaule.keycloaknats.consumer;

import io.nats.client.Connection;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import java.time.Duration;
import java.util.List;

/** Keeps pull requests alive while the client reconnects without buffering those requests. */
final class ConsumerPull {
  private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(2);
  private static final long RECONNECT_BACKOFF_MS = 250;
  // jnats uses the same exception type for fatal subscription errors and buffer rejection.
  private static final String RECONNECT_BUFFER_REJECTED =
      "Unable to queue any more messages during reconnect, max buffer is 0";

  private ConsumerPull() {}

  static List<Message> fetch(Connection connection, JetStreamSubscription subscription)
      throws InterruptedException {
    try {
      return subscription.fetch(1, FETCH_TIMEOUT);
    } catch (IllegalStateException failure) {
      Connection.Status status = connection.getStatus();
      if (failure.getClass() != IllegalStateException.class
          || !RECONNECT_BUFFER_REJECTED.equals(failure.getMessage())
          || (status != Connection.Status.DISCONNECTED
              && status != Connection.Status.RECONNECTING
              && status != Connection.Status.CONNECTED)) {
        throw failure;
      }
      // CONNECTED also covers a reconnect completing between rejection and this status check.
      // Infinite reconnects are configured on the connection. Bound the retry rate and let
      // shutdown interrupt this wait; a fresh pull is safe because no request was buffered.
      Thread.sleep(RECONNECT_BACKOFF_MS);
      return List.of();
    }
  }
}
