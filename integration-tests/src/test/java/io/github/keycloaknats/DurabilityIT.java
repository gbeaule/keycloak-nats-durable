package io.github.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.keycloaknats.consumer.InboxProcessor;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.StreamConfiguration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.*;

/**
 * Real processes, disk-backed JetStream, PostgreSQL and an installed Keycloak provider. No disabled
 * tests.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DurabilityIT extends IntegrationSupport {
  @BeforeAll
  static void start() throws Exception {
    startInfrastructure();
    InboxProcessor.initialize(database);
  }

  @AfterAll
  static void stop() throws Exception {
    stopInfrastructure();
  }

  @BeforeEach
  void reset(TestInfo test) throws Exception {
    System.out.println("Testing: " + test.getDisplayName());
    loginAdmin();
    drained();
    nats.jetStreamManagement().deleteConsumer(STREAM, DURABLE);
    nats.jetStreamManagement().purgeStream(STREAM);
    consumer();
    execute("TRUNCATE knd_inbox, knd_effects");
  }

  @AfterEach
  void logs() throws Exception {
    saveLogs();
  }

  List<Message> fetch(int count) throws Exception {
    var subscription = nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
    try {
      return subscription.fetch(count, Duration.ofSeconds(5));
    } finally {
      subscription.unsubscribe();
    }
  }

  static JsonNode event(Message message) throws Exception {
    return JSON.readTree(message.getData());
  }

  @Test
  @Order(1)
  void loginAndFailedLoginAreDurableWithoutKeycloaksOwnEventStore() throws Exception {
    assertEquals(
        200,
        login(
                "durable-test",
                "grant_type=password&client_id=test-client&username=alice&password=alice-password")
            .statusCode());
    assertEquals(
        400,
        login(
                "durable-test",
                "grant_type=password&client_id=test-client&username=alice&password=incorrect")
            .statusCode());
    drained();
    var messages = fetch(2);
    assertEquals(2, messages.size());
    var types = new HashSet<String>();
    for (var message : messages) {
      var json = event(message);
      types.add(json.at("/data/eventType").asText());
      assertEquals(json.get("id").asText(), message.getHeaders().getFirst("Nats-Msg-Id"));
      assertFalse(
          new String(message.getData(), java.nio.charset.StandardCharsets.UTF_8)
              .contains("alice-password"));
      message.ackSync(Duration.ofSeconds(2));
    }
    assertEquals(java.util.Set.of("LOGIN", "LOGIN_ERROR"), types);
  }

  @Test
  @Order(2)
  void accountCreateDisableAndDeleteHaveTargetIdentityWithoutRepresentations() throws Exception {
    String id = createUser();
    assertEquals(
        204,
        request("PUT", "/admin/realms/durable-test/users/" + id, Map.of("enabled", false))
            .statusCode());
    assertEquals(
        204, request("DELETE", "/admin/realms/durable-test/users/" + id, null).statusCode());
    drained();
    var messages = fetch(3);
    assertEquals(3, messages.size());
    var ops = new HashSet<String>();
    for (var message : messages) {
      var data = event(message).get("data");
      assertEquals(id, data.get("userId").asText());
      String op = data.get("operationType").asText();
      ops.add(op);
      if (op.equals("UPDATE")) assertFalse(data.get("userEnabled").asBoolean());
      assertFalse(data.has("representation"));
      message.ackSync(Duration.ofSeconds(2));
    }
    assertEquals(java.util.Set.of("CREATE", "UPDATE", "DELETE"), ops);
  }

  @Test
  @Order(3)
  void unacknowledgedMessageGoesToAnotherWorkerAndAckedMessageDoesNotReturn() throws Exception {
    createUser();
    drained();
    var first = fetch(1).getFirst(); // This worker exits without acknowledgement.
    var second = fetch(1).getFirst();
    assertEquals(event(first).get("id"), event(second).get("id"));
    assertTrue(second.metaData().deliveredCount() >= 2);
    second.ackSync(Duration.ofSeconds(2));
    var subscription = nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
    try {
      assertTrue(subscription.fetch(1, Duration.ofSeconds(1)).isEmpty());
    } finally {
      subscription.unsubscribe();
    }
  }

  @Test
  @Order(4)
  void databaseFailureRollsBackTheAccountChange() throws Exception {
    String id = createUser();
    drained();
    execute(
        "ALTER TABLE kc_nats_outbox ADD CONSTRAINT injected_failure CHECK (payload NOT LIKE '%"
            + id
            + "%') NOT VALID");
    try {
      var failed =
          request("PUT", "/admin/realms/durable-test/users/" + id, Map.of("enabled", false));
      assertTrue(
          failed.statusCode() >= 400,
          "Capture failure must reject the account update: "
              + failed.statusCode()
              + " "
              + failed.body());
      assertEquals(
          1, scalar("SELECT count(*) FROM user_entity WHERE id='" + id + "' AND enabled=true"));
      var user = request("GET", "/admin/realms/durable-test/users/" + id, null);
      assertEquals(200, user.statusCode());
      assertTrue(JSON.readTree(user.body()).get("enabled").asBoolean());
      assertEquals(1, messages(), "Only the committed CREATE may be published");
    } finally {
      execute("ALTER TABLE kc_nats_outbox DROP CONSTRAINT injected_failure");
    }
  }

  @Test
  @Order(5)
  void brokerOutageAndHardKeycloakRestartDoNotLoseCommittedChanges() throws Exception {
    var docker = broker.getDockerClient();
    docker.stopContainerCmd(broker.getContainerId()).withTimeout(2).exec();
    var ids = new HashSet<String>();
    try {
      for (int i = 0; i < 5; i++) ids.add(createUser());
      assertEquals(5, scalar("SELECT count(*) FROM kc_nats_outbox"));
      restartKeycloak(); // Restarts while NATS is still unavailable.
      assertEquals(5, scalar("SELECT count(*) FROM kc_nats_outbox"));
    } finally {
      docker.startContainerCmd(broker.getContainerId()).exec();
    }
    await()
        .atMost(Duration.ofSeconds(15))
        .ignoreExceptions()
        .untilAsserted(IntegrationSupport::connectNats);
    drained();
    assertEquals(5, messages());
    var received = new HashSet<String>();
    for (var message : fetch(5)) {
      received.add(event(message).at("/data/userId").asText());
      message.ackSync(Duration.ofSeconds(2));
    }
    assertEquals(ids, received);
  }

  @Test
  @Order(6)
  void crashAfterBrokerAckBeforeDatabaseCommitRetriesTheSameId() throws Exception {
    execute(
        "CREATE FUNCTION knd_delay_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(10); RETURN OLD; END $$");
    execute(
        "CREATE TRIGGER knd_delay_delete BEFORE DELETE ON kc_nats_outbox FOR EACH ROW EXECUTE FUNCTION knd_delay_delete()");
    try {
      createUser();
      await()
          .atMost(Duration.ofSeconds(20))
          .until(
              () ->
                  messages() == 1
                      && scalar(
                              "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'")
                          > 0);
      assertEquals(1, scalar("SELECT count(*) FROM kc_nats_outbox"));
      keycloak
          .getDockerClient()
          .killContainerCmd(keycloak.getContainerId())
          .withSignal("KILL")
          .exec();
      execute("DROP TRIGGER knd_delay_delete ON kc_nats_outbox");
      keycloak.getDockerClient().startContainerCmd(keycloak.getContainerId()).exec();
      await()
          .atMost(Duration.ofMinutes(2))
          .ignoreExceptions()
          .until(() -> request("GET", "/realms/durable-test", null).statusCode() == 200);
      loginAdmin();
      drained();
      assertEquals(1, messages(), "Retried publication must be deduplicated");
    } finally {
      execute("DROP TRIGGER IF EXISTS knd_delay_delete ON kc_nats_outbox");
      execute("DROP FUNCTION IF EXISTS knd_delay_delete()");
    }
  }

  @Test
  @Order(7)
  void fullStreamRetainsOverflowInOutboxUntilConsumerMakesSpace() throws Exception {
    var original = nats.jetStreamManagement().getStreamInfo(STREAM).getConfiguration();
    nats.jetStreamManagement()
        .updateStream(StreamConfiguration.builder(original).maxMessages(1).build());
    try {
      createUser();
      createUser();
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> messages() == 1 && scalar("SELECT count(*) FROM kc_nats_outbox") == 1);
      fetch(1).getFirst().ackSync(Duration.ofSeconds(2));
      drained();
      assertEquals(1, messages());
      fetch(1).getFirst().ackSync(Duration.ofSeconds(2));
    } finally {
      nats.jetStreamManagement().updateStream(original);
    }
  }

  @Test
  @Order(8)
  void unsafeStreamIsRejectedUntilItsConfigurationIsFixed() throws Exception {
    var original = nats.jetStreamManagement().getStreamInfo(STREAM).getConfiguration();
    nats.jetStreamManagement()
        .updateStream(
            StreamConfiguration.builder(original).discardPolicy(DiscardPolicy.Old).build());
    try {
      createUser();
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> scalar("SELECT count(*) FROM kc_nats_outbox WHERE attempts > 0") == 1);
      assertEquals(0, messages());
    } finally {
      nats.jetStreamManagement().updateStream(original);
    }
    drained();
    assertEquals(1, messages());
  }

  @Test
  @Order(9)
  void consumerCrashAfterCommitBeforeAckDoesNotRepeatBusinessEffects() throws Exception {
    createUser();
    drained();
    InboxProcessor processor = new InboxProcessor(database, DURABLE);
    var first = fetch(1).getFirst();
    assertTrue(processor.process(first.getData(), processor::recordEffect));
    // Simulated consumer death here: database commit completed, no ACK was sent.
    var repeated = fetch(1).getFirst();
    assertFalse(processor.process(repeated.getData(), processor::recordEffect));
    repeated.ackSync(Duration.ofSeconds(2));
    assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
  }

  @Test
  @Order(10)
  void consumerFailureRollsBackInboxAndEffectsTogether() throws Exception {
    createUser();
    drained();
    var message = fetch(1).getFirst();
    InboxProcessor processor = new InboxProcessor(database, DURABLE);
    assertThrows(
        IllegalStateException.class,
        () ->
            processor.process(
                message.getData(),
                (db, event) -> {
                  processor.recordEffect(db, event);
                  throw new IllegalStateException("injected business failure");
                }));
    assertEquals(0, scalar("SELECT count(*) FROM knd_inbox"));
    assertEquals(0, scalar("SELECT count(*) FROM knd_effects"));
    assertTrue(processor.process(message.getData(), processor::recordEffect));
    message.ackSync(Duration.ofSeconds(2));
  }

  @Test
  @Order(11)
  void concurrentDuplicateDeliveriesProduceOneEffect() throws Exception {
    createUser();
    drained();
    var message = fetch(1).getFirst();
    InboxProcessor processor = new InboxProcessor(database, DURABLE);
    try (var pool = Executors.newFixedThreadPool(8)) {
      var futures = new ArrayList<java.util.concurrent.Future<Boolean>>();
      for (int i = 0; i < 16; i++)
        futures.add(
            pool.submit(() -> processor.process(message.getData(), processor::recordEffect)));
      int applied = 0;
      for (var future : futures) if (future.get()) applied++;
      assertEquals(1, applied);
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
    }
    message.ackSync(Duration.ofSeconds(2));
  }

  @Test
  @Order(12)
  void conflictingPayloadForExistingIdIsNotAcknowledgedAsDuplicate() throws Exception {
    createUser();
    drained();
    var message = fetch(1).getFirst();
    InboxProcessor processor = new InboxProcessor(database, DURABLE);
    processor.process(message.getData(), processor::recordEffect);
    var changed = (com.fasterxml.jackson.databind.node.ObjectNode) event(message);
    changed.put("type", "io.keycloak.user.logout");
    assertThrows(
        IllegalStateException.class,
        () -> processor.process(JSON.writeValueAsBytes(changed), processor::recordEffect));
    assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
    message.ackSync(Duration.ofSeconds(2));
  }

  @Test
  @Order(13)
  void twoKeycloakNodesDrainABacklogWithoutDoublePublication() throws Exception {
    var second = keycloakContainer(false);
    try {
      second.start();
      try (var pool = Executors.newFixedThreadPool(6)) {
        var futures = new ArrayList<java.util.concurrent.Future<String>>();
        for (int i = 0; i < 24; i++) futures.add(pool.submit(IntegrationSupport::createUser));
        for (var future : futures) assertNotNull(future.get());
      }
      drained();
      assertEquals(24, messages());
      var unique = new HashSet<String>();
      for (var message : fetch(24)) {
        unique.add(event(message).get("id").asText());
        message.ackSync(Duration.ofSeconds(2));
      }
      assertEquals(24, unique.size());
    } finally {
      if (second.getContainerId() != null)
        java.nio.file.Files.writeString(
            java.nio.file.Path.of("target/keycloak-second-node.log"), second.getLogs());
      second.stop();
    }
  }

  @Test
  @Order(14)
  void hardBrokerRestartPreservesMessagesAndDurableAcknowledgements() throws Exception {
    createUser();
    createUser();
    drained();
    var batch = fetch(2);
    assertEquals(2, batch.size());
    batch.getFirst().ackSync(Duration.ofSeconds(2));
    String unackedId = event(batch.get(1)).get("id").asText();
    nats.close();
    var docker = broker.getDockerClient();
    docker.killContainerCmd(broker.getContainerId()).withSignal("KILL").exec();
    docker.startContainerCmd(broker.getContainerId()).exec();
    await()
        .atMost(Duration.ofSeconds(15))
        .ignoreExceptions()
        .untilAsserted(IntegrationSupport::connectNats);
    assertEquals(1, messages());
    var redelivered = fetch(1).getFirst();
    assertEquals(unackedId, event(redelivered).get("id").asText());
    redelivered.ackSync(Duration.ofSeconds(2));
    assertEquals(0, messages());
  }

  @Test
  @Order(15)
  void inboxStillDeduplicatesAfterJetStreamsDeduplicationWindowExpires() throws Exception {
    var original = nats.jetStreamManagement().getStreamInfo(STREAM).getConfiguration();
    nats.jetStreamManagement()
        .updateStream(
            StreamConfiguration.builder(original).duplicateWindow(Duration.ofMillis(500)).build());
    try {
      createUser();
      drained();
      var first = fetch(1).getFirst();
      InboxProcessor processor = new InboxProcessor(database, DURABLE);
      assertTrue(processor.process(first.getData(), processor::recordEffect));
      first.ackSync(Duration.ofSeconds(2));
      await()
          .atMost(Duration.ofSeconds(5))
          .until(
              () ->
                  !nats.jetStream()
                      .publish(
                          first.getSubject(),
                          first.getData(),
                          io.nats.client.PublishOptions.builder()
                              .expectedStream(STREAM)
                              .messageId(event(first).get("id").asText())
                              .build())
                      .isDuplicate());
      var replay = fetch(1).getFirst();
      assertFalse(processor.process(replay.getData(), processor::recordEffect));
      replay.ackSync(Duration.ofSeconds(2));
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
    } finally {
      nats.jetStreamManagement().updateStream(original);
    }
  }
}
