package io.github.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamOptions;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class JetStreamPublisherTest {
  private final BridgeConfig config = BridgeConfig.from(Map.of());
  private final OutboxEvent event =
      new OutboxEvent("id", "keycloak.events.realm.user.login", "{}", 0);
  private Connection connection;
  private JetStreamPublisher.Connector connector;
  private JetStreamManagement management;
  private JetStream jetStream;
  private JetStreamPublisher publisher;

  @BeforeEach
  void setup() throws Exception {
    connection = mock(Connection.class);
    connector = mock(JetStreamPublisher.Connector.class);
    management = mock(JetStreamManagement.class);
    jetStream = mock(JetStream.class);
    when(connector.connect(any())).thenReturn(connection);
    when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
    when(connection.jetStreamManagement(any(JetStreamOptions.class))).thenReturn(management);
    when(connection.jetStream(any(JetStreamOptions.class))).thenReturn(jetStream);
    var stream = mock(StreamInfo.class);
    when(stream.getConfiguration()).thenReturn(StreamSafetyTest.safe().build());
    when(management.getStreamInfo(config.stream())).thenReturn(stream);
    var ack = mock(PublishAck.class);
    when(ack.getStream()).thenReturn(config.stream());
    when(ack.getSeqno()).thenReturn(1L);
    when(jetStream.publish(
            anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class)))
        .thenReturn(ack);
    publisher = new JetStreamPublisher(config, connector);
  }

  @Test
  void closedPublisherNeverConnects() {
    publisher.close();
    assertThrows(IOException.class, () -> publisher.publish(event));
    verifyNoInteractions(connector);
  }

  @Test
  void closeDuringConnectDoesNotWaitAndClosesTheLateConnection() throws Exception {
    var connecting = new CountDownLatch(1);
    var finishConnect = new CountDownLatch(1);
    when(connector.connect(any()))
        .thenAnswer(
            call -> {
              connecting.countDown();
              assertTrue(finishConnect.await(3, TimeUnit.SECONDS));
              return connection;
            });
    var executor = Executors.newFixedThreadPool(2);
    try {
      final var publishing =
          executor.submit(
              () -> {
                publisher.publish(event);
                return null;
              });
      assertTrue(connecting.await(2, TimeUnit.SECONDS));
      executor.submit(publisher::close).get(1, TimeUnit.SECONDS);
      finishConnect.countDown();
      var failure =
          assertThrows(ExecutionException.class, () -> publishing.get(2, TimeUnit.SECONDS));
      assertInstanceOf(IOException.class, failure.getCause());
      verify(connection).close();
      verifyNoInteractions(management, jetStream);
      assertThrows(IOException.class, () -> publisher.publish(event));
      verify(connector).connect(any());
    } finally {
      finishConnect.countDown();
      publisher.close();
      executor.shutdownNow();
    }
  }

  enum RequestStage {
    VALIDATION,
    PUBLICATION
  }

  @ParameterizedTest
  @EnumSource(RequestStage.class)
  void closeCanCancelAnInFlightRequest(RequestStage stage) throws Exception {
    var requesting = new CountDownLatch(1);
    var releaseRequest = new CountDownLatch(1);
    org.mockito.stubbing.Answer<Object> pendingRequest =
        call -> {
          requesting.countDown();
          assertTrue(releaseRequest.await(3, TimeUnit.SECONDS));
          throw new IOException("Request cancelled by connection close");
        };
    if (stage == RequestStage.VALIDATION) {
      when(management.getStreamInfo(config.stream())).thenAnswer(pendingRequest);
    } else {
      when(jetStream.publish(
              anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class)))
          .thenAnswer(pendingRequest);
    }
    doAnswer(
            call -> {
              releaseRequest.countDown();
              return null;
            })
        .when(connection)
        .close();
    var executor = Executors.newFixedThreadPool(2);
    try {
      final var publishing =
          executor.submit(
              () -> {
                publisher.publish(event);
                return null;
              });
      assertTrue(requesting.await(2, TimeUnit.SECONDS));
      executor.submit(publisher::close).get(1, TimeUnit.SECONDS);
      var failure =
          assertThrows(ExecutionException.class, () -> publishing.get(2, TimeUnit.SECONDS));
      assertInstanceOf(IOException.class, failure.getCause());
      verify(connection).close();
    } finally {
      releaseRequest.countDown();
      publisher.close();
      executor.shutdownNow();
    }
  }

  @Test
  void simultaneousFirstPublishesShareOneConnection() throws Exception {
    var connecting = new CountDownLatch(1);
    var finishConnect = new CountDownLatch(1);
    var secondStarted = new CountDownLatch(1);
    when(connector.connect(any()))
        .thenAnswer(
            call -> {
              connecting.countDown();
              assertTrue(finishConnect.await(3, TimeUnit.SECONDS));
              return connection;
            });
    var executor = Executors.newFixedThreadPool(2);
    try {
      final var first =
          executor.submit(
              () -> {
                publisher.publish(event);
                return null;
              });
      assertTrue(connecting.await(2, TimeUnit.SECONDS));
      final var second =
          executor.submit(
              () -> {
                secondStarted.countDown();
                publisher.publish(event);
                return null;
              });
      assertTrue(secondStarted.await(2, TimeUnit.SECONDS));
      finishConnect.countDown();
      first.get(2, TimeUnit.SECONDS);
      second.get(2, TimeUnit.SECONDS);
      verify(connector).connect(any());
      verify(jetStream, times(2))
          .publish(
              eq(event.subject()),
              any(Headers.class),
              any(byte[].class),
              any(PublishOptions.class));
    } finally {
      finishConnect.countDown();
      publisher.close();
      executor.shutdownNow();
    }
  }

  @Test
  void failedConnectAllowsFreshAttempt() throws Exception {
    when(connector.connect(any())).thenThrow(new IOException("Unavailable")).thenReturn(connection);
    assertThrows(IOException.class, () -> publisher.publish(event));
    publisher.publish(event);
    verify(connector, times(2)).connect(any());
    publisher.close();
  }

  @Test
  void repeatedCloseReleasesConnectionOnlyOnce() throws Exception {
    publisher.publish(event);
    publisher.close();
    publisher.close();
    verify(connection).close();
  }

  @Test
  void interruptedClosePreservesInterrupt() throws Exception {
    publisher.publish(event);
    doThrow(new InterruptedException()).when(connection).close();
    try {
      publisher.close();
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }
}
