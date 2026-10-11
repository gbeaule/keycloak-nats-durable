package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.gbeaule.keycloaknats.jetstream.StreamPolicy;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamOptions;
import io.nats.client.NKey;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.PublishAck;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;

class JetStreamPublisherTest {
  private final BridgeConfig config = functionalConfig(Map.of());
  private final OutboxEvent event =
      CaptureFixtures.row("id", "keycloak.events.realm.user.login", "{}", 0);
  private Connection connection;
  private JetStreamPublisher.Connector connector;
  private JetStreamManagement management;
  private JetStream jetStream;
  private JetStreamPublisher publisher;
  private PublishAck ack;
  @TempDir Path directory;

  private static BridgeConfig functionalConfig(Map<String, String> overrides) {
    var settings = new HashMap<>(overrides);
    // Allow cold class loading and instrumentation on CI; deadline tests use explicit budgets.
    settings.put("timeout-ms", "10000");
    return BridgeConfig.from(settings);
  }

  private static StreamConfiguration.Builder safeStream() {
    return StreamConfiguration.builder()
        .name("KEYCLOAK_EVENTS")
        .subjects("keycloak.events.>")
        .storageType(StorageType.File)
        .replicas(3)
        .retentionPolicy(RetentionPolicy.WorkQueue)
        .discardPolicy(DiscardPolicy.New)
        .duplicateWindow(Duration.ofMinutes(2));
  }

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
    when(connection.getMaxPayload()).thenReturn(1048576L);
    when(connection.jetStreamManagement(any(JetStreamOptions.class))).thenReturn(management);
    when(connection.jetStream(any(JetStreamOptions.class))).thenReturn(jetStream);
    var stream = mock(StreamInfo.class);
    when(stream.getConfiguration()).thenReturn(safeStream().build());
    when(management.getStreamInfo(config.stream())).thenReturn(stream);
    ack = mock(PublishAck.class);
    when(ack.getStream()).thenReturn(config.stream());
    when(ack.getSeqno()).thenReturn(1L);
    when(jetStream.publish(
            anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class)))
        .thenReturn(ack);
    publisher = new JetStreamPublisher(config, connector);
  }

  @AfterEach
  void closePublisher() {
    publisher.close();
  }

  @Test
  void validatesBeforePublishingOriginalBytesAndDeduplicationIdentityWithBoundedRequests()
      throws Exception {
    var clock = new AtomicLong();
    Duration connectTime = Duration.ofSeconds(3);
    Duration validationTime = Duration.ofSeconds(1);
    var info = safeStreamInfo();
    when(connector.connect(any()))
        .thenAnswer(
            call -> {
              clock.addAndGet(connectTime.toNanos());
              return connection;
            });
    when(management.getStreamInfo(config.stream()))
        .thenAnswer(
            call -> {
              clock.addAndGet(validationTime.toNanos());
              return info;
            });
    publisher = new JetStreamPublisher(config, connector, clock::get);
    var original =
        CaptureFixtures.row(
            "persisted-id", "keycloak.events.realm.user.login", "{\"value\":\"é\"}", 0);
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
    var connectionOptions = ArgumentCaptor.forClass(Options.class);
    verify(connector).connect(connectionOptions.capture());
    assertEquals(config.timeout(), connectionOptions.getValue().getConnectionTimeout());
    var request = ArgumentCaptor.forClass(JetStreamOptions.class);
    verify(connection).jetStreamManagement(request.capture());
    Duration managementBudget = request.getValue().getRequestTimeout();
    assertEquals(config.timeout().minus(connectTime), managementBudget);
    verify(connection).jetStream(request.capture());
    assertEquals(managementBudget.minus(validationTime), request.getValue().getRequestTimeout());
  }

  @Test
  void everyPublicationRevalidatesTheStreamAndUnsafeChangesPreventSending() throws Exception {
    publisher.publish(event);
    var changed = mock(StreamInfo.class);
    when(changed.getConfiguration())
        .thenReturn(safeStream().storageType(StorageType.Memory).build());
    when(management.getStreamInfo(config.stream())).thenReturn(changed);
    assertThrows(StreamPolicy.Violation.class, () -> publisher.publish(event));
    verify(management, times(2)).getStreamInfo(config.stream());
    verify(jetStream)
        .publish(anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class));
  }

  @Test
  void negotiatedServerLimitIncludesHeadersAndIsRecheckedAfterReconnect() throws Exception {
    when(connection.getMaxPayload()).thenReturn(config.maxPayloadBytes() + 512L);
    publisher.publish(event);
    when(connection.getMaxPayload()).thenReturn(config.maxPayloadBytes() + 511L);
    var failure = assertThrows(StreamPolicy.Violation.class, () -> publisher.publish(event));
    assertEquals(
        "Server max_payload must leave room for the configured payload and headers",
        failure.getMessage());
    verify(jetStream)
        .publish(anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class));
    verify(management).getStreamInfo(config.stream());
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
    when(replacement.getMaxPayload()).thenReturn(1048576L);
    when(replacement.jetStreamManagement(any())).thenReturn(management);
    when(replacement.jetStream(any())).thenReturn(jetStream);
    when(connector.connect(any())).thenReturn(replacement);
    publisher.publish(event);
    publisher.close();
    verify(connector, times(2)).connect(any());
    verify(replacement, timeout(1000)).close();
    verify(connection, never()).close();
  }

  @Test
  void connectionOptionsPreserveTokenAndDisableUnacknowledgedReconnectBuffering() throws Exception {
    var settings = functionalConfig(Map.of("token", " private-token "));
    try (var configured = new JetStreamPublisher(settings, connector)) {
      configured.publish(event);
      var options = ArgumentCaptor.forClass(Options.class);
      verify(connector).connect(options.capture());
      assertEquals(" private-token ", options.getValue().getToken());
      assertTrue(options.getValue().getConnectionTimeout().compareTo(Duration.ZERO) > 0);
      assertTrue(options.getValue().getConnectionTimeout().compareTo(settings.timeout()) <= 0);
      assertEquals(-1, options.getValue().getMaxReconnect());
      assertEquals(0, options.getValue().getReconnectBufferSize());
      assertEquals("keycloak-nats-durable", options.getValue().getConnectionName());
      assertInstanceOf(NatsDiagnostics.class, options.getValue().getErrorListener());
      assertInstanceOf(NatsDiagnostics.class, options.getValue().getConnectionListener());
    }
  }

  @Test
  void tlsUsesVerifiedContextWithoutClientCredentials() throws Exception {
    var settings = functionalConfig(Map.of("nats-url", "tls://broker:4222"));
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
    var settings = functionalConfig(Map.of("credentials-file", credentials.toString()));
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
        functionalConfig(Map.of("nats-url", "tls://broker:4222", "tls-ca-file", empty.toString()));
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
              awaitIgnoringInterrupt(finishConnect);
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
      verify(connection, timeout(1000)).close();
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
      verify(connection, timeout(1000)).close();
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
    verify(connection, timeout(1000)).close();
  }

  @Test
  void interruptedClosePreservesInterrupt() throws Exception {
    publisher.publish(event);
    doThrow(new InterruptedException()).when(connection).close();
    try {
      Thread.currentThread().interrupt();
      publisher.close();
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
    verify(connection, timeout(1000)).close();
  }

  @Test
  void anInterruptedCallerNeverStartsNetworkWork() {
    try {
      Thread.currentThread().interrupt();
      assertThrows(InterruptedException.class, () -> publisher.publish(event));
      verifyNoInteractions(connector, management, jetStream);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void fatalNetworkTaskFailuresReachTheOwner() throws Exception {
    var failure = new AssertionError("fatal network failure");
    when(connector.connect(any())).thenThrow(failure);
    assertSame(failure, assertThrows(AssertionError.class, () -> publisher.publish(event)));
  }

  @Test
  void totalBudgetIncludesConnectionAndStreamValidationBeforePublishing() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    when(connector.connect(any()))
        .thenAnswer(
            call -> {
              Thread.sleep(75);
              return connection;
            });
    when(management.getStreamInfo(config.stream()))
        .thenAnswer(
            call -> {
              entered.countDown();
              awaitIgnoringInterrupt(release);
              return safeStreamInfo();
            });
    var bounded = new JetStreamPublisher(BridgeConfig.from(Map.of("timeout-ms", "250")), connector);
    try {
      long started = System.nanoTime();
      assertThrows(SocketTimeoutException.class, () -> bounded.publish(event));
      assertEquals(0, entered.getCount());
      assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
      verify(connection, timeout(1000)).close();
      // Cancellation must be checked after the late metadata response, before any original send.
      release.countDown();
      bounded.close();
      verify(jetStream, never())
          .publish(anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class));
    } finally {
      release.countDown();
      bounded.close();
    }
  }

  @Test
  void elapsedValidationBudgetCannotRenewThePublishBudget() throws Exception {
    var clock = new AtomicLong();
    when(management.getStreamInfo(config.stream()))
        .thenAnswer(
            call -> {
              clock.set(config.timeout().toNanos());
              return safeStreamInfo();
            });
    try (var bounded = new JetStreamPublisher(config, connector, clock::get)) {
      assertThrows(SocketTimeoutException.class, () -> bounded.publish(event));
      verifyNoInteractions(jetStream);
    }
  }

  @Test
  void timedOutConnectCannotAccumulateTasksOrInstallItsLateConnection() throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    when(connector.connect(any()))
        .thenAnswer(
            call -> {
              entered.countDown();
              awaitIgnoringInterrupt(release);
              return connection;
            });
    try (var bounded =
        new JetStreamPublisher(BridgeConfig.from(Map.of("timeout-ms", "150")), connector)) {
      assertThrows(SocketTimeoutException.class, () -> bounded.publish(event));
      assertEquals(0, entered.getCount());
      for (int retry = 0; retry < 10; retry++) {
        assertThrows(IOException.class, () -> bounded.publish(event));
      }
      verify(connector).connect(any());
      verifyNoInteractions(management, jetStream);
      release.countDown();
      verify(connection, timeout(1000)).close();
      when(connector.connect(any())).thenReturn(connection);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
      while (true) {
        try {
          bounded.publish(event);
          break;
        } catch (IOException completing) {
          assertTrue(System.nanoTime() < deadline, completing::toString);
          Thread.sleep(1);
        }
      }
      verify(connector, times(2)).connect(any());
      verify(jetStream)
          .publish(anyString(), any(Headers.class), any(byte[].class), any(PublishOptions.class));
    } finally {
      release.countDown();
    }
  }

  @Test
  void slowConnectionCleanupCannotExtendTheDeadlineOrAccumulateMoreTasks() throws Exception {
    var closing = new CountDownLatch(1);
    var releaseClose = new CountDownLatch(1);
    when(management.getStreamInfo(config.stream()))
        .thenAnswer(
            call -> {
              Thread.sleep(5000);
              return safeStreamInfo();
            });
    doAnswer(
            call -> {
              closing.countDown();
              awaitIgnoringInterrupt(releaseClose);
              return null;
            })
        .when(connection)
        .close();
    try (var bounded =
        new JetStreamPublisher(BridgeConfig.from(Map.of("timeout-ms", "150")), connector)) {
      long started = System.nanoTime();
      assertThrows(SocketTimeoutException.class, () -> bounded.publish(event));
      assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
      assertTrue(closing.await(1, TimeUnit.SECONDS));
      for (int retry = 0; retry < 100; retry++) {
        assertThrows(IOException.class, () -> bounded.publish(event));
        Thread.sleep(1);
      }
      verify(connector).connect(any());
      verify(connection).close();
      verifyNoInteractions(jetStream);
    } finally {
      releaseClose.countDown();
    }
  }

  @Test
  void callerInterruptionCancelsItsAttemptAndClosesTheConnection() throws Exception {
    var requesting = new CountDownLatch(1);
    when(management.getStreamInfo(config.stream()))
        .thenAnswer(
            call -> {
              requesting.countDown();
              Thread.sleep(5000);
              return safeStreamInfo();
            });
    var outcome = new java.util.concurrent.CompletableFuture<Exception>();
    Thread owner =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    publisher.publish(event);
                    outcome.complete(null);
                  } catch (Exception failure) {
                    outcome.complete(failure);
                  }
                });
    try {
      assertTrue(requesting.await(1, TimeUnit.SECONDS));
      owner.interrupt();
      assertInstanceOf(InterruptedException.class, outcome.get(1, TimeUnit.SECONDS));
      verify(connection, timeout(1000)).close();
      verifyNoInteractions(jetStream);
    } finally {
      owner.interrupt();
      owner.join(1000);
    }
  }

  private static StreamInfo safeStreamInfo() {
    var info = mock(StreamInfo.class);
    when(info.getConfiguration()).thenReturn(safeStream().build());
    return info;
  }

  private static void awaitIgnoringInterrupt(CountDownLatch release) {
    boolean finished = false;
    while (!finished) {
      try {
        finished = release.await(3, TimeUnit.SECONDS);
        assertTrue(finished);
      } catch (InterruptedException ignored) {
        // Model DNS, credential loading or an operation that does not support interruption.
      }
    }
  }
}
