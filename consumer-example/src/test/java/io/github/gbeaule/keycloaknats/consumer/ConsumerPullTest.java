package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.JetStreamStatusException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.support.Status;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ConsumerPullTest {
  private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(2);

  private static IllegalStateException reconnectFailure() {
    return new IllegalStateException(
        "Unable to queue any more messages during reconnect, max buffer is 0");
  }

  @Test
  void successfulPullReturnsMessagesWithoutConsultingConnectionStatus() throws Exception {
    var connection = mock(Connection.class);
    var subscription = mock(JetStreamSubscription.class);
    var messages = List.of(mock(Message.class));
    when(subscription.fetch(1, FETCH_TIMEOUT)).thenReturn(messages);

    assertSame(messages, ConsumerPull.fetch(connection, subscription));
    verifyNoInteractions(connection);
  }

  @ParameterizedTest
  @EnumSource(
      value = Connection.Status.class,
      names = {"DISCONNECTED", "RECONNECTING"})
  void rejectedPullCanBeRetriedAfterReconnection(Connection.Status status) throws Exception {
    var connection = mock(Connection.class);
    var subscription = mock(JetStreamSubscription.class);
    var messages = List.of(mock(Message.class));
    when(connection.getStatus()).thenReturn(status);
    when(subscription.fetch(1, FETCH_TIMEOUT)).thenThrow(reconnectFailure()).thenReturn(messages);

    assertTrue(ConsumerPull.fetch(connection, subscription).isEmpty());
    when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
    assertSame(messages, ConsumerPull.fetch(connection, subscription));
  }

  @ParameterizedTest
  @EnumSource(
      value = Connection.Status.class,
      names = {"CONNECTING", "CLOSED"})
  void bufferRejectionOutsideReconnectionRemainsFatal(Connection.Status status) {
    var connection = mock(Connection.class);
    var subscription = mock(JetStreamSubscription.class);
    var failure = reconnectFailure();
    when(connection.getStatus()).thenReturn(status);
    when(subscription.fetch(1, FETCH_TIMEOUT)).thenThrow(failure);

    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class, () -> ConsumerPull.fetch(connection, subscription)));
  }

  @Test
  void reconnectCompletingBeforeTheFailureIsCheckedStillAllowsTheNextPull() throws Exception {
    var connection = mock(Connection.class);
    var subscription = mock(JetStreamSubscription.class);
    var messages = List.of(mock(Message.class));
    when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
    when(subscription.fetch(1, FETCH_TIMEOUT)).thenThrow(reconnectFailure()).thenReturn(messages);

    assertTrue(ConsumerPull.fetch(connection, subscription).isEmpty());
    assertSame(messages, ConsumerPull.fetch(connection, subscription));
  }

  @Test
  void fatalSubscriptionAndServerFailuresRemainFatalDuringReconnection() {
    var connection = mock(Connection.class);
    when(connection.getStatus()).thenReturn(Connection.Status.RECONNECTING);
    for (var failure :
        List.of(
            new IllegalStateException("Subscription is inactive"),
            new IllegalStateException(),
            new JetStreamStatusException(new Status(409, "Consumer Deleted")))) {
      var subscription = mock(JetStreamSubscription.class);
      when(subscription.fetch(1, FETCH_TIMEOUT)).thenThrow(failure);
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class, () -> ConsumerPull.fetch(connection, subscription)));
    }
  }

  @Test
  void shutdownInterruptPreventsWaitingOrRetryingTheRejectedPull() throws Exception {
    var connection = mock(Connection.class);
    var subscription = mock(JetStreamSubscription.class);
    var attempted = new CountDownLatch(1);
    var release = new Semaphore(0);
    var failure = new AtomicReference<Throwable>();
    when(connection.getStatus()).thenReturn(Connection.Status.DISCONNECTED);
    when(subscription.fetch(1, FETCH_TIMEOUT))
        .thenAnswer(
            invocation -> {
              attempted.countDown();
              release.acquireUninterruptibly();
              throw reconnectFailure();
            });
    var worker =
        new Thread(
            () -> {
              try {
                ConsumerPull.fetch(connection, subscription);
              } catch (Throwable stopped) {
                failure.set(stopped);
              }
            });
    worker.start();
    try {
      assertTrue(attempted.await(5, TimeUnit.SECONDS));
      worker.interrupt();
      release.release();
      worker.join(5000);
      assertFalse(worker.isAlive());
      assertInstanceOf(InterruptedException.class, failure.get());
    } finally {
      worker.interrupt();
      release.release();
      worker.join(5000);
    }
  }
}
