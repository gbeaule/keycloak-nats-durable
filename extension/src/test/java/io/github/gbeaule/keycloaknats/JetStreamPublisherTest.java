package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamOptions;
import io.nats.client.NKey;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

class JetStreamPublisherTest {
  private final BridgeConfig config = BridgeConfig.from(Map.of());
  private final OutboxEvent event =
      new OutboxEvent("id", "keycloak.events.realm.user.login", "{}", 0);
  private Connection connection;
  private JetStreamPublisher.Connector connector;
  private JetStreamManagement management;
  private JetStream jetStream;
  private JetStreamPublisher publisher;
  private PublishAck ack;
  @TempDir Path directory;

  @Test
  void defaultPublisherConnectsLazilyAndRejectsPublicationAfterClose() throws Exception {
    try (var nats = mockStatic(Nats.class)) {
      var unused = new JetStreamPublisher(config);
      unused.close();
      unused.close();
      var failure = assertThrows(IOException.class, () -> unused.publish(event));
      assertEquals("Publisher is closed", failure.getMessage());
      nats.verifyNoInteractions();
    }
  }

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
    ack = mock(PublishAck.class);
    when(ack.getStream()).thenReturn(config.stream());
    when(ack.getSeqno()).thenReturn(1L);
    when(jetStream.publish(
            anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class)))
        .thenReturn(ack);
    publisher = new JetStreamPublisher(config, connector);
  }

  @Test
  void validatesBeforePublishingOriginalBytesAndDeduplicationIdentityWithBoundedRequests()
      throws Exception {
    var original =
        new OutboxEvent("persisted-id", "keycloak.events.realm.user.login", "{\"value\":\"é\"}", 0);
    publisher.publish(original);
    var headers = ArgumentCaptor.forClass(Headers.class);
    var bytes = ArgumentCaptor.forClass(byte[].class);
    var options = ArgumentCaptor.forClass(PublishOptions.class);
    var order = inOrder(management, jetStream);
    order.verify(management).getStreamInfo(config.stream());
    order
        .verify(jetStream)
        .publish(eq(original.subject()), headers.capture(), bytes.capture(), options.capture());
    assertArrayEquals(original.payload().getBytes(StandardCharsets.UTF_8), bytes.getValue());
    assertEquals(
        Map.of("Content-Type", List.of("application/cloudevents+json")),
        headers.getValue().toMap());
    assertEquals(original.id(), options.getValue().getMessageId());
    assertEquals(config.stream(), options.getValue().getExpectedStream());
    assertNull(options.getValue().getMessageTtl());
    var request = ArgumentCaptor.forClass(JetStreamOptions.class);
    verify(connection).jetStreamManagement(request.capture());
    assertEquals(config.timeout(), request.getValue().getRequestTimeout());
    verify(connection).jetStream(request.capture());
    assertEquals(config.timeout(), request.getValue().getRequestTimeout());
  }

  @Test
  void everyPublicationRevalidatesTheStreamAndUnsafeChangesPreventSending() throws Exception {
    publisher.publish(event);
    var changed = mock(StreamInfo.class);
    when(changed.getConfiguration())
        .thenReturn(StreamSafetyTest.safe().storageType(StorageType.Memory).build());
    when(management.getStreamInfo(config.stream())).thenReturn(changed);
    assertThrows(UnsafeStreamException.class, () -> publisher.publish(event));
    verify(management, times(2)).getStreamInfo(config.stream());
    verify(jetStream)
        .publish(anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class));
  }

  @ParameterizedTest
  @CsvSource({"OTHER,1", "KEYCLOAK_EVENTS,0", "KEYCLOAK_EVENTS,-1"})
  void wrongDestinationOrInvalidSequenceCannotCountAsAcknowledgement(String stream, long sequence) {
    when(ack.getStream()).thenReturn(stream);
    when(ack.getSeqno()).thenReturn(sequence);
    var failure = assertThrows(IOException.class, () -> publisher.publish(event));
    assertEquals("Unexpected publish acknowledgement", failure.getMessage());
  }

  @Test
  void duplicateAcknowledgementStillConfirmsThePersistedMessage() throws Exception {
    when(ack.isDuplicate()).thenReturn(true);
    publisher.publish(event);
    var options = ArgumentCaptor.forClass(PublishOptions.class);
    verify(jetStream)
        .publish(eq(event.subject()), any(Headers.class), any(byte[].class), options.capture());
    assertEquals(event.id(), options.getValue().getMessageId());
  }

  @Test
  void reconnectingConnectionIsReusedWithoutBufferingAnotherPublication() throws Exception {
    publisher.publish(event);
    when(connection.getStatus()).thenReturn(Connection.Status.RECONNECTING);
    var failure = assertThrows(IOException.class, () -> publisher.publish(event));
    assertEquals("NATS is unavailable", failure.getMessage());
    verify(connector).connect(any());
    verify(management).getStreamInfo(config.stream());
    verify(jetStream)
        .publish(anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class));
  }

  @Test
  void closedConnectionIsReplacedOnTheNextAttempt() throws Exception {
    publisher.publish(event);
    when(connection.getStatus()).thenReturn(Connection.Status.CLOSED);
    var replacement = mock(Connection.class);
    when(replacement.getStatus()).thenReturn(Connection.Status.CONNECTED);
    when(replacement.jetStreamManagement(any())).thenReturn(management);
    when(replacement.jetStream(any())).thenReturn(jetStream);
    when(connector.connect(any())).thenReturn(replacement);
    publisher.publish(event);
    publisher.close();
    verify(connector, times(2)).connect(any());
    verify(replacement).close();
    verify(connection, never()).close();
  }

  @Test
  void connectionOptionsPreserveTokenAndDisableUnacknowledgedReconnectBuffering() throws Exception {
    var settings = BridgeConfig.from(Map.of("token", " private-token ", "timeout-ms", "1234"));
    try (var configured = new JetStreamPublisher(settings, connector)) {
      configured.publish(event);
      var options = ArgumentCaptor.forClass(Options.class);
      verify(connector).connect(options.capture());
      assertEquals(" private-token ", options.getValue().getToken());
      assertEquals(settings.timeout(), options.getValue().getConnectionTimeout());
      assertEquals(-1, options.getValue().getMaxReconnect());
      assertEquals(0, options.getValue().getReconnectBufferSize());
      assertEquals("keycloak-nats-durable", options.getValue().getConnectionName());
      assertInstanceOf(NatsDiagnostics.class, options.getValue().getErrorListener());
      assertInstanceOf(NatsDiagnostics.class, options.getValue().getConnectionListener());
    }
  }

  @Test
  void tlsUsesVerifiedContextWithoutClientCredentials() throws Exception {
    var settings = BridgeConfig.from(Map.of("nats-url", "tls://broker:4222"));
    try (var configured = new JetStreamPublisher(settings, connector)) {
      configured.publish(event);
      var options = ArgumentCaptor.forClass(Options.class);
      verify(connector).connect(options.capture());
      assertNotNull(options.getValue().getSslContext());
      assertNull(options.getValue().getAuthHandler());
    }
  }

  @Test
  void credentialsFileProvidesTheJwtAndSignsWithItsPrivateKey() throws Exception {
    NKey key = NKey.createUser(new SecureRandom());
    Path credentials = directory.resolve("publisher.creds");
    Files.writeString(
        credentials,
        "-----BEGIN NATS USER JWT-----\nunit-test-jwt\n------END NATS USER JWT------\n"
            + "-----BEGIN USER NKEY SEED-----\n"
            + new String(key.getSeed())
            + "\n------END USER NKEY SEED------\n");
    var settings = BridgeConfig.from(Map.of("credentials-file", credentials.toString()));
    try (var configured = new JetStreamPublisher(settings, connector)) {
      configured.publish(event);
      var options = ArgumentCaptor.forClass(Options.class);
      verify(connector).connect(options.capture());
      var authentication = options.getValue().getAuthHandler();
      assertNotNull(authentication);
      assertEquals("unit-test-jwt", new String(authentication.getJWT()));
      byte[] nonce = "server-challenge".getBytes(StandardCharsets.UTF_8);
      assertTrue(key.verify(nonce, authentication.sign(nonce)));
      assertNull(options.getValue().getToken());
    } finally {
      key.clear();
    }
  }

  @Test
  void invalidTlsTrustMaterialFailsBeforeConnectingWithoutLeakingParserDetails() throws Exception {
    Path empty = directory.resolve("private-ca.pem");
    Files.writeString(empty, "");
    var settings =
        BridgeConfig.from(Map.of("nats-url", "tls://broker:4222", "tls-ca-file", empty.toString()));
    try (var configured = new JetStreamPublisher(settings, connector)) {
      var failure = assertThrows(IOException.class, () -> configured.publish(event));
      assertEquals("Cannot load verified NATS TLS configuration", failure.getMessage());
      assertNull(failure.getCause());
      verifyNoInteractions(connector, management, jetStream);
    }
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
