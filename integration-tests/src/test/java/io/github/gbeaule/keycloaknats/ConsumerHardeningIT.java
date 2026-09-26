package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gbeaule.keycloaknats.consumer.ConsumerDatabase;
import io.github.gbeaule.keycloaknats.consumer.ConsumerMonitor;
import io.github.gbeaule.keycloaknats.consumer.ConsumerSettings;
import io.github.gbeaule.keycloaknats.consumer.ConsumerWorker;
import io.github.gbeaule.keycloaknats.consumer.FailurePolicy;
import io.github.gbeaule.keycloaknats.consumer.InboxProcessor;
import io.github.gbeaule.keycloaknats.consumer.OutboxReport;
import io.github.gbeaule.keycloaknats.consumer.ProcessingLimits;
import io.github.gbeaule.keycloaknats.consumer.QuarantineStore;
import io.github.gbeaule.keycloaknats.consumer.RejectedEventException;
import io.nats.client.Connection;
import io.nats.client.JetStreamOptions;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/** Exercises real transaction deadlines, broker flow control and durable operator recovery. */
@Testcontainers
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class ConsumerHardeningIT {
  private static final String STREAM = "CONSUMER_TEST";
  private static final String DURABLE = "worker";
  private static final String SUBJECT = "keycloak.events.realm.user.login";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Container
  private static final PostgreSQLContainer postgres =
      IntegrationSupport.postgresContainer()
          .withDatabaseName("consumer")
          .withUsername("consumer")
          .withPassword("integration-only");

  @Container
  private static final GenericContainer<?> broker =
      new GenericContainer<>(IntegrationSupport.NATS_IMAGE)
          .withExposedPorts(4222)
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("nats.conf"), "/etc/nats/test.conf")
          .withCommand("-c", "/etc/nats/test.conf");

  private static PGSimpleDataSource database;
  private static Connection nats;

  @BeforeAll
  static void start() throws Exception {
    database = new PGSimpleDataSource();
    database.setURL(postgres.getJdbcUrl());
    database.setUser(postgres.getUsername());
    database.setPassword(postgres.getPassword());
    ProcessingLimits.defaults().configure(database);
    InboxProcessor.initialize(database);
    nats = Nats.connect("nats://" + broker.getHost() + ":" + broker.getMappedPort(4222));
    nats.jetStreamManagement()
        .addStream(
            StreamConfiguration.builder()
                .name(STREAM)
                .subjects("keycloak.events.>")
                .storageType(StorageType.File)
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .replicas(1)
                .duplicateWindow(Duration.ofSeconds(2))
                .build());
  }

  @AfterAll
  static void stop() throws Exception {
    if (nats != null) {
      nats.close();
    }
  }

  @BeforeEach
  void reset() throws Exception {
    if (nats.jetStreamManagement().getConsumerNames(STREAM).contains(DURABLE)) {
      nats.jetStreamManagement().deleteConsumer(STREAM, DURABLE);
    }
    nats.jetStreamManagement().purgeStream(STREAM);
    nats.jetStreamManagement()
        .createConsumer(
            STREAM,
            ConsumerConfiguration.builder()
                .durable(DURABLE)
                .filterSubject("keycloak.events.>")
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.All)
                .ackWait(Duration.ofMillis(400))
                .maxDeliver(-1)
                .maxAckPending(1)
                .build());
    execute("TRUNCATE knd_inbox,knd_effects,knd_quarantine");
  }

  @Test
  void statementTimeoutRollsBackInboxAndBusinessEffectTogether() throws Exception {
    byte[] payload = payload();
    var processor =
        new InboxProcessor(database, DURABLE, new ProcessingLimits(1, 2, 200, 100, 5000, 1000));
    long started = System.nanoTime();
    assertThrows(
        SQLException.class,
        () ->
            processor.process(
                payload,
                (db, event) -> {
                  processor.recordEffect(db, event);
                  try (var query = db.createStatement()) {
                    query.execute("SELECT pg_sleep(10)");
                  }
                }));
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 4000);
    assertEquals(0, scalar("SELECT count(*) FROM knd_inbox"));
    assertEquals(0, scalar("SELECT count(*) FROM knd_effects"));
    assertTrue(processor.process(payload, processor::recordEffect));
  }

  @Test
  void cancellationRetiresThePooledConnectionAndTheNextDeliveryCanCommit() throws Exception {
    byte[] payload = payload();
    var limits = new ProcessingLimits(1, 2, 5000, 1000, 500, 1000);
    try (var pool = ConsumerDatabase.pool(database, limits, 1)) {
      var processor = new InboxProcessor(pool, DURABLE, limits);
      assertThrows(
          java.util.concurrent.TimeoutException.class,
          () ->
              processor.process(
                  payload,
                  (db, event) -> {
                    processor.recordEffect(db, event);
                    try (var query = db.createStatement()) {
                      query.execute("SELECT pg_sleep(10)");
                    }
                  }));
      assertEquals(0, scalar("SELECT count(*) FROM knd_inbox"));
      assertEquals(0, scalar("SELECT count(*) FROM knd_effects"));
      // The server may still notice the closed socket asynchronously. Its statement bound releases
      // locks; use a fresh processing deadline while retrying through the same bounded pool.
      var next = new InboxProcessor(pool, DURABLE, ProcessingLimits.defaults());
      await()
          .atMost(Duration.ofSeconds(10))
          .ignoreExceptions()
          .until(() -> next.process(payload, next::recordEffect));
      assertFalse(next.process(payload, next::recordEffect));
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
    } finally {
      ProcessingLimits.defaults().configure(database);
    }
  }

  @Test
  void backlogCollectorUsesOnlyReadPrivilegesAndSupportsRealmAndTopicScopes() throws Exception {
    execute(
        """
        CREATE TABLE kc_nats_outbox
          (id text,subject text,created_at bigint,next_attempt_at bigint,
           attempts bigint,payload text);
        INSERT INTO kc_nats_outbox VALUES
          ('1','keycloak.events.cmVhbG0.user.login',1000,0,3,'private'),
          ('2','keycloak.events.cmVhbG0.admin.user.create',1000,0,1,'private'),
          ('3','keycloak.events.b3RoZXI.user.login',1000,0,0,'private');
        CREATE ROLE report_reader LOGIN PASSWORD 'integration-only';
        GRANT USAGE ON SCHEMA public TO report_reader;
        GRANT SELECT (subject,created_at,next_attempt_at,attempts)
          ON kc_nats_outbox TO report_reader;
        """);
    var reader = new PGSimpleDataSource();
    reader.setURL(postgres.getJdbcUrl());
    reader.setUser("report_reader");
    reader.setPassword("integration-only");
    ProcessingLimits.defaults().configure(reader);
    var report =
        OutboxReport.collect(
            reader, "public", "keycloak.events.*.user.*", "keycloak.events.cmVhbG0.", 5);
    assertEquals(1L, report.get("pending"));
    assertEquals(3L, report.get("max_attempts"));
    assertEquals(1L, report.get("due"));
    assertTrue(report.get("table_bytes").longValue() > 0);
    assertTrue(OutboxReport.prometheus(report).contains("knd_outbox_scrape_success 1"));
    assertEquals(
        0L,
        OutboxReport.collect(reader, "public", "keycloak.events.*.user.logout", "", 5)
            .get("pending"));
    assertEquals(3, scalar("SELECT count(*) FROM kc_nats_outbox"));
  }

  @Test
  void lockTimeoutRetainsDeliveryEvenWithDropEnabled() throws Exception {
    publish(payload());
    Message message = fetch();
    var settings =
        settings(
            Map.of(
                "KND_CONSUMER_FAILURE_ACTION",
                "drop",
                "KND_CONSUMER_FAILURE_SUBJECTS",
                ">",
                "KND_CONSUMER_FAILURE_MIN_DELIVERIES",
                "1",
                "KND_CONSUMER_LOCK_MS",
                "100"));
    var processor = new InboxProcessor(database, DURABLE, settings.processing());
    try (var blocker = database.getConnection();
        var query = blocker.createStatement();
        var monitor = new ConsumerMonitor(settings, () -> true);
        var worker = worker(processor, settings, monitor)) {
      blocker.setAutoCommit(false);
      query.execute("LOCK TABLE knd_inbox IN ACCESS EXCLUSIVE MODE");
      worker.handle(message, processor::recordEffect);
      assertEquals(1, monitor.count(ConsumerMonitor.Counter.RETRIES));
      assertEquals(0, monitor.count(ConsumerMonitor.Counter.DROPPED));
      assertEquals(1, messages());
      assertEquals(0, scalar("SELECT count(*) FROM knd_quarantine"));
      blocker.rollback();
      worker.handle(fetch(), processor::recordEffect);
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
      assertEquals(0, messages());
    }
  }

  @Test
  void defaultRetryKeepsPoisonMessageAndQuarantineFreesThePendingWindow() throws Exception {
    publish("not-json".getBytes(StandardCharsets.UTF_8));
    publish(payload());
    var defaults = settings(Map.of());
    var processor = new InboxProcessor(database, DURABLE, defaults.processing());
    try (var monitor = new ConsumerMonitor(defaults, () -> true);
        var worker = worker(processor, defaults, monitor)) {
      worker.handle(fetch(), processor::recordEffect);
      assertEquals(1, monitor.count(ConsumerMonitor.Counter.RETRIES));
      assertEquals(2, messages());
      assertEquals(0, scalar("SELECT count(*) FROM knd_quarantine"));
    }
    var quarantine =
        settings(
            Map.of(
                "KND_CONSUMER_FAILURE_ACTION",
                "quarantine",
                "KND_CONSUMER_FAILURE_SUBJECTS",
                ">",
                "KND_CONSUMER_FAILURE_MIN_DELIVERIES",
                "1"));
    try (var monitor = new ConsumerMonitor(quarantine, () -> true);
        var worker = worker(processor, quarantine, monitor)) {
      worker.handle(fetch(), processor::recordEffect);
      assertEquals(1, monitor.count(ConsumerMonitor.Counter.QUARANTINED));
      assertEquals(1, messages());
      assertEquals(1, scalar("SELECT count(*) FROM knd_quarantine WHERE payload IS NOT NULL"));
      worker.handle(fetch(), processor::recordEffect);
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
      assertEquals(0, messages());
    }
  }

  @Test
  void changedPolicyCannotAcknowledgeDiscardAuditAsAQuarantineCopy() throws Exception {
    publish("not-json".getBytes(StandardCharsets.UTF_8));
    final var message = fetch();
    var settings =
        settings(
            Map.of(
                "KND_CONSUMER_FAILURE_ACTION", "quarantine",
                "KND_CONSUMER_FAILURE_SUBJECTS", ">",
                "KND_CONSUMER_FAILURE_MIN_DELIVERIES", "1"));
    var store = new QuarantineStore(database, DURABLE, settings.processing());
    // Simulate a prior drop policy committing its audit, followed by a crash before ACK.
    store.record(message, FailurePolicy.Action.DROP, "INVALID_JSON");
    var processor = new InboxProcessor(database, DURABLE, settings.processing());
    try (var monitor = new ConsumerMonitor(settings, () -> true);
        var worker = worker(processor, settings, monitor)) {
      worker.handle(message, processor::recordEffect);
      assertEquals(1, messages());
      assertEquals(1, monitor.count(ConsumerMonitor.Counter.RETRIES));
      assertEquals(0, monitor.count(ConsumerMonitor.Counter.QUARANTINED));
      assertTrue(store.pending(100).isEmpty());
    }
  }

  @Test
  void runtimeRoleProcessesAndQuarantinesWithoutSchemaOwnership() throws Exception {
    // PostgreSQL 14 grants PUBLIC schema creation by default; upgraded databases may retain it.
    execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
    execute("CREATE ROLE knd_runtime LOGIN PASSWORD 'integration-only'");
    execute("GRANT USAGE ON SCHEMA public TO knd_runtime");
    execute("GRANT SELECT,INSERT ON knd_inbox,knd_quarantine TO knd_runtime");
    execute("GRANT INSERT ON knd_effects TO knd_runtime");
    var runtime = new PGSimpleDataSource();
    runtime.setURL(postgres.getJdbcUrl());
    runtime.setUser("knd_runtime");
    runtime.setPassword("integration-only");
    ProcessingLimits.defaults().configure(runtime);
    var processor = new InboxProcessor(runtime, DURABLE);
    byte[] event = payload();
    assertTrue(processor.process(event, processor::recordEffect));
    assertFalse(processor.process(event, processor::recordEffect));
    publish("not-json".getBytes(StandardCharsets.UTF_8));
    new QuarantineStore(runtime, DURABLE, ProcessingLimits.defaults())
        .record(fetch(), FailurePolicy.Action.QUARANTINE, "INVALID_JSON");
    assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
    assertEquals(1, scalar("SELECT count(*) FROM knd_quarantine WHERE payload IS NOT NULL"));
    try (var db = runtime.getConnection();
        var query = db.createStatement()) {
      var denied =
          assertThrows(
              SQLException.class,
              () -> query.execute("CREATE TABLE public.forbidden_runtime_ddl(id int)"));
      assertEquals("42501", denied.getSQLState());
    }
  }

  @Test
  void stalledDatabaseSocketIsBoundedAndRetainsDelivery() throws Exception {
    publish(payload());
    final var settings =
        settings(
            Map.of(
                "KND_CONSUMER_SOCKET_SECONDS",
                "1",
                "KND_CONSUMER_DEADLINE_MS",
                "5000",
                "KND_CONSUMER_FAILURE_ACTION",
                "drop",
                "KND_CONSUMER_FAILURE_SUBJECTS",
                ">",
                "KND_CONSUMER_FAILURE_MIN_DELIVERIES",
                "1"));
    var stalledDatabase = new PGSimpleDataSource();
    stalledDatabase.setURL(postgres.getJdbcUrl());
    stalledDatabase.setUser(postgres.getUsername());
    stalledDatabase.setPassword(postgres.getPassword());
    settings.processing().configure(stalledDatabase);
    var processor = new InboxProcessor(stalledDatabase, DURABLE, settings.processing());
    var entered = new CountDownLatch(1);
    Message message = fetch();
    try (var monitor = new ConsumerMonitor(settings, () -> true);
        var worker = worker(processor, settings, monitor);
        var pool = Executors.newSingleThreadExecutor()) {
      var result =
          pool.submit(
              () -> {
                worker.handle(
                    message,
                    (db, event) -> {
                      processor.recordEffect(db, event);
                      entered.countDown();
                      try (var query = db.createStatement()) {
                        query.execute("SELECT pg_sleep(30)");
                      }
                    });
                return null;
              });
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      postgres.getDockerClient().pauseContainerCmd(postgres.getContainerId()).exec();
      try {
        result.get(5, TimeUnit.SECONDS);
        assertEquals(1, monitor.count(ConsumerMonitor.Counter.RETRIES));
        assertEquals(0, monitor.count(ConsumerMonitor.Counter.DROPPED));
        assertEquals(1, messages());
      } finally {
        postgres.getDockerClient().unpauseContainerCmd(postgres.getContainerId()).exec();
      }
      assertEquals(0, scalar("SELECT count(*) FROM knd_effects"));
      assertEquals(0, scalar("SELECT count(*) FROM knd_inbox"));
      // A silent backend can retain locks until it notices the closed socket or statement timeout.
      await()
          .atMost(Duration.ofSeconds(15))
          .until(
              () -> {
                worker.handle(fetch(), processor::recordEffect);
                return monitor.count(ConsumerMonitor.Counter.COMMITTED) == 1;
              });
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
    }
  }

  @Test
  void acknowledgementFailureAfterCommitNeverTriggersDropping() throws Exception {
    publish(payload());
    Message original = fetch();
    Message failedAck =
        (Message)
            java.lang.reflect.Proxy.newProxyInstance(
                Message.class.getClassLoader(),
                new Class<?>[] {Message.class},
                (proxy, method, arguments) -> {
                  if ("ackSync".equals(method.getName())) {
                    throw new java.util.concurrent.TimeoutException("injected ACK failure");
                  }
                  return method.invoke(original, arguments);
                });
    var settings =
        settings(
            Map.of(
                "KND_CONSUMER_FAILURE_ACTION",
                "drop",
                "KND_CONSUMER_FAILURE_SUBJECTS",
                ">",
                "KND_CONSUMER_FAILURE_MIN_DELIVERIES",
                "1"));
    var processor = new InboxProcessor(database, DURABLE, settings.processing());
    try (var monitor = new ConsumerMonitor(settings, () -> true);
        var worker = worker(processor, settings, monitor)) {
      worker.handle(failedAck, processor::recordEffect);
      assertEquals(1, monitor.count(ConsumerMonitor.Counter.ACK_FAILURES));
      assertEquals(0, monitor.count(ConsumerMonitor.Counter.DROPPED));
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
      assertEquals(0, scalar("SELECT count(*) FROM knd_quarantine"));
      worker.handle(fetch(), processor::recordEffect);
      assertEquals(1, monitor.count(ConsumerMonitor.Counter.DUPLICATES));
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
      assertEquals(0, messages());
    }
  }

  @Test
  void failedQuarantineWriteNeverAcknowledgesTheOriginal() throws Exception {
    publish("not-json".getBytes(StandardCharsets.UTF_8));
    var settings =
        settings(
            Map.of(
                "KND_CONSUMER_FAILURE_ACTION",
                "quarantine",
                "KND_CONSUMER_FAILURE_SUBJECTS",
                ">",
                "KND_CONSUMER_FAILURE_MIN_DELIVERIES",
                "1",
                "KND_CONSUMER_LOCK_MS",
                "100"));
    var processor = new InboxProcessor(database, DURABLE, settings.processing());
    try (var blocker = database.getConnection();
        var query = blocker.createStatement();
        var monitor = new ConsumerMonitor(settings, () -> true);
        var worker = worker(processor, settings, monitor)) {
      blocker.setAutoCommit(false);
      query.execute("LOCK TABLE knd_quarantine IN ACCESS EXCLUSIVE MODE");
      worker.handle(fetch(), processor::recordEffect);
      assertEquals(1, monitor.count(ConsumerMonitor.Counter.RETRIES));
      assertEquals(1, messages());
      blocker.rollback();
      assertEquals(0, scalar("SELECT count(*) FROM knd_quarantine"));
    }
  }

  @Test
  void explicitDroppingRetainsAuditMetadataWithoutThePayload() throws Exception {
    publish("not-json".getBytes(StandardCharsets.UTF_8));
    var settings =
        settings(
            Map.of(
                "KND_CONSUMER_FAILURE_ACTION",
                "drop",
                "KND_CONSUMER_FAILURE_SUBJECTS",
                SUBJECT,
                "KND_CONSUMER_FAILURE_MIN_DELIVERIES",
                "1"));
    var processor = new InboxProcessor(database, DURABLE, settings.processing());
    try (var monitor = new ConsumerMonitor(settings, () -> true);
        var worker = worker(processor, settings, monitor)) {
      worker.handle(fetch(), processor::recordEffect);
      assertEquals(1, monitor.count(ConsumerMonitor.Counter.DROPPED));
      assertEquals(0, messages());
      assertEquals(
          1,
          scalar(
              "SELECT count(*) FROM knd_quarantine"
                  + " WHERE disposition='drop' AND payload IS NULL AND headers_json IS NULL"));
      assertEquals(0, scalar("SELECT count(*) FROM knd_effects"));
    }
  }

  @Test
  void progressAcknowledgementsPreventConcurrentDeliveryOfLongRunningWork() throws Exception {
    publish(payload());
    var settings = settings(Map.of());
    var processor = new InboxProcessor(database, DURABLE, settings.processing());
    var processing = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var monitor = new ConsumerMonitor(settings, () -> true);
        var worker = worker(processor, settings, monitor);
        var pool = Executors.newSingleThreadExecutor()) {
      Message first = fetch();
      var result =
          pool.submit(
              () -> {
                worker.handle(
                    first,
                    (db, event) -> {
                      processing.countDown();
                      assertTrue(release.await(5, TimeUnit.SECONDS));
                      processor.recordEffect(db, event);
                    });
                return null;
              });
      try {
        assertTrue(processing.await(2, TimeUnit.SECONDS));
        var second = nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
        try {
          assertTrue(
              second.fetch(1, Duration.ofMillis(1200)).isEmpty(),
              "No redelivery across three AckWait periods while progress is sent");
          assertTrue(monitor.metrics().contains("knd_consumer_active 1"));
        } finally {
          second.unsubscribe();
        }
      } finally {
        release.countDown();
      }
      result.get(5, TimeUnit.SECONDS);
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
      assertEquals(0, messages());
    }
  }

  @Test
  void replayDoesNotMistakeDeduplicationForRestoringAnAcknowledgedMessage() throws Exception {
    byte[] original = payload();
    String id = objectMapper.readTree(original).path("id").asText();
    nats.jetStream().publish(SUBJECT, original, PublishOptions.builder().messageId(id).build());
    Message rejected = fetch();
    long sequence = rejected.metaData().streamSequence();
    var settings =
        settings(
            Map.of(
                "KND_CONSUMER_FAILURE_ACTION",
                "quarantine",
                "KND_CONSUMER_FAILURE_SUBJECTS",
                ">",
                "KND_CONSUMER_FAILURE_MIN_DELIVERIES",
                "1"));
    var processor = new InboxProcessor(database, DURABLE, settings.processing());
    var store = new QuarantineStore(database, DURABLE, settings.processing());
    try (var monitor = new ConsumerMonitor(settings, () -> true);
        var worker = worker(processor, settings, monitor)) {
      worker.handle(
          rejected,
          (db, event) -> {
            throw new RejectedEventException(RejectedEventException.Reason.HANDLER_REJECTED);
          });
      assertEquals(0, messages());
      var jetStream =
          nats.jetStream(JetStreamOptions.builder().requestTimeout(Duration.ofSeconds(1)).build());
      assertFalse(store.replay(jetStream, STREAM, sequence));
      assertEquals(1, store.pending(100).size());
      await().atMost(Duration.ofSeconds(6)).until(() -> store.replay(jetStream, STREAM, sequence));
      assertTrue(store.pending(100).isEmpty());
      Message replay = fetch();
      assertArrayEquals(original, replay.getData());
      assertEquals(id, replay.getHeaders().getFirst("Nats-Msg-Id"));
      worker.handle(replay, processor::recordEffect);
      assertEquals(1, scalar("SELECT count(*) FROM knd_effects"));
    }
  }

  private static ConsumerSettings settings(Map<String, String> overrides) {
    var values = new java.util.HashMap<>(overrides);
    values.put("KND_CONSUMER_PROGRESS_MS", "50");
    return ConsumerSettings.from(values::get);
  }

  private static ConsumerWorker worker(
      InboxProcessor processor, ConsumerSettings settings, ConsumerMonitor monitor) {
    return new ConsumerWorker(
        processor,
        new QuarantineStore(database, DURABLE, settings.processing()),
        settings,
        monitor);
  }

  private static byte[] payload() throws Exception {
    return objectMapper.writeValueAsBytes(
        Map.of(
            "specversion",
            "1.0",
            "id",
            UUID.randomUUID(),
            "source",
            "urn:keycloak:realm:test",
            "type",
            "io.keycloak.user.login",
            "dataschema",
            "urn:keycloak-nats:event:v1",
            "data",
            Map.of()));
  }

  private static void publish(byte[] payload) throws Exception {
    nats.jetStream().publish(SUBJECT, payload);
  }

  private static Message fetch() throws Exception {
    var subscription = nats.jetStream().subscribe(null, PullSubscribeOptions.bind(STREAM, DURABLE));
    try {
      return subscription.fetch(1, Duration.ofSeconds(3)).getFirst();
    } finally {
      subscription.unsubscribe();
    }
  }

  private static long messages() throws Exception {
    return nats.jetStreamManagement().getStreamInfo(STREAM).getStreamState().getMsgCount();
  }

  private static void execute(String sql) throws Exception {
    try (var db = database.getConnection();
        var query = db.createStatement()) {
      query.execute(sql);
    }
  }

  private static long scalar(String sql) throws Exception {
    try (var db = database.getConnection();
        var query = db.createStatement();
        var result = query.executeQuery(sql)) {
      result.next();
      return result.getLong(1);
    }
  }
}
