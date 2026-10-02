package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Local discard uses PostgreSQL ownership; only original events ever enter JetStream. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OutboxDiscardIT extends RelayIntegrationSupport {
  @ParameterizedTest
  @CsvSource({"1,audit", "64,audit", "1,delete", "64,delete", "1,commit", "64,commit"})
  void rejectedDiscardDoesNotStarveOtherUsersAcrossWorkers(int batchSize, String failure)
      throws Exception {
    capture("expired", "blocked", new PublicationPolicy(1, null), 2000);
    capture("successor", "blocked");
    capture("other", "healthy");
    rejectDiscard(failure);
    var workerConfig =
        BridgeConfig.from(
            Map.of(
                "batch-size", Integer.toString(batchSize),
                "retry-initial-ms", "60000",
                "retry-max-ms", "60000"));
    try (var actual = new JetStreamPublisher(config)) {
      var firstWorker = new OutboxRelay(OutboxDiscardIT::transaction, actual, workerConfig);
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, firstWorker.runBatch().outcome());
      // A different worker must observe the cooldown through PostgreSQL.
      var secondWorker = new OutboxRelay(OutboxDiscardIT::transaction, actual, workerConfig);
      assertEquals(1, secondWorker.runBatch().published());
      assertNotNull(row("expired"));
      assertEquals(0, row("expired").attempts());
      assertFalse(row("expired").publicationMayHaveOccurred());
      assertFalse(row("successor").publicationMayHaveOccurred());
      assertEquals(0, audits());
      assertEquals(List.of("other"), storedIds(1));
      assertTrue(row("expired").nextExpiryAttemptAt() > row("expired").expiresAt());

      // Another failed retry must also leave room for newly captured users.
      change("expired", row -> row.deferResolution(0));
      capture("another", "another-healthy");
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, firstWorker.runBatch().outcome());
      assertEquals(1, secondWorker.runBatch().published());
      assertFalse(row("successor").publicationMayHaveOccurred());
      assertEquals(0, audits());
      assertEquals(List.of("other", "another"), storedIds(2));
    } finally {
      allowDiscard(failure);
    }
    change("expired", row -> row.deferResolution(0));
    try (var actual = new JetStreamPublisher(config)) {
      var result = new OutboxRelay(OutboxDiscardIT::transaction, actual, config).runBatch();
      assertEquals(1, result.expired());
      assertEquals(1, result.published());
    }
    assertAudit("expired", "EXPIRED", 0, false);
    assertEquals(List.of("other", "another", "successor"), storedIds(3));
  }

  @Test
  void pollingWorkerMakesProgressAfterDiscardCooldownHasAlreadyElapsed() throws Exception {
    capture("expired", "blocked", new PublicationPolicy(1, null), 2000);
    capture("successor", "blocked");
    capture("other", "healthy");
    rejectDiscard("audit");
    var workerConfig =
        BridgeConfig.from(
            Map.of(
                "batch-size", "1",
                "poll-ms", "200",
                "idle-poll-max-ms", "200",
                "retry-initial-ms", "1",
                "retry-max-ms", "1"));
    var wakeup = new RelayWakeup();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var actual = new JetStreamPublisher(config)) {
      var relay = new OutboxRelay(OutboxDiscardIT::transaction, actual, workerConfig);
      var running = executor.submit(new RelayWorker(relay, wakeup, workerConfig));
      try {
        await().atMost(Duration.ofSeconds(10)).until(() -> row("other") == null);
      } finally {
        relay.stop();
        wakeup.close();
        running.get(10, TimeUnit.SECONDS);
      }
      assertNotNull(row("expired"));
      assertFalse(row("successor").publicationMayHaveOccurred());
      assertEquals(0, audits());
      assertEquals(List.of("other"), storedIds(1));
    } finally {
      allowDiscard("audit");
    }
  }

  @Test
  void discardBackoffDoesNotOverwriteNewerOwnership() throws Exception {
    capture("expired", "changed", new PublicationPolicy(1, null), 2000);
    final var original = row("expired");
    rejectDiscard("audit");
    var transactions = new AtomicInteger();
    try {
      var result =
          new OutboxRelay(
                  work -> {
                    int call = transactions.incrementAndGet();
                    try {
                      transaction(work);
                    } catch (RuntimeException failure) {
                      if (call == 1) {
                        transaction(
                            em ->
                                em.find(OutboxEvent.class, "expired")
                                    .deferResolution(Long.MAX_VALUE));
                      }
                      throw failure;
                    }
                  },
                  neverPublish(),
                  config)
              .runBatch();
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, result.outcome());
      assertEquals(2, transactions.get());
      assertEquals(Long.MAX_VALUE, row("expired").nextAttemptAt());
      assertEquals(Long.MAX_VALUE, row("expired").nextExpiryAttemptAt());
      assertEquals(original.version() + 1, row("expired").version());
      assertEquals(0, audits());
    } finally {
      allowDiscard("audit");
    }
  }

  @Test
  void uncertainDiscardCommitDoesNotDeferTheSuccessor() throws Exception {
    capture("expired", "committed", new PublicationPolicy(1, null), 2000);
    capture("successor", "committed");
    var transactions = new AtomicInteger();
    try (var actual = new JetStreamPublisher(config)) {
      var relay =
          new OutboxRelay(
              work -> {
                transaction(work);
                if (transactions.incrementAndGet() == 1) {
                  throw new IllegalStateException("discard commit acknowledgement lost");
                }
              },
              actual,
              config);
      var result = relay.runBatch();
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, result.outcome());
      assertEquals(0, result.discarded());
      assertNull(row("expired"));
      assertEquals(row("successor").createdAt(), row("successor").nextAttemptAt());
      assertNull(row("successor").nextExpiryAttemptAt());
      assertEquals(1, audits());
      assertEquals(1, relay.runBatch().published());
    }
    assertEquals(List.of("successor"), storedIds(1));
  }

  @Test
  void failedDiscardAfterSendPreservesIntentAndLetsOtherUsersPublish() throws Exception {
    capture("abandoned", "ambiguous", new PublicationPolicy(null, 1), 2000);
    capture("successor", "ambiguous");
    capture("other", "healthy");
    rejectDiscard("audit");
    try (var actual = new JetStreamPublisher(config)) {
      var relay =
          new OutboxRelay(
              OutboxDiscardIT::transaction,
              publisher(
                  event -> {
                    actual.publish(event);
                    if (event.id().equals("abandoned")) {
                      throw new IOException("ack lost");
                    }
                  }),
              config);
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, relay.runBatch().outcome());
      assertTrue(row("abandoned").publicationMayHaveOccurred());
      assertEquals(0, row("abandoned").attempts());
      assertTrue(row("abandoned").nextAttemptAt() > row("abandoned").createdAt());
      assertEquals(1, relay.runBatch().published());
      assertFalse(row("successor").publicationMayHaveOccurred());
      assertEquals(0, audits());
      assertEquals(List.of("abandoned", "other"), storedIds(2));
      allowDiscard("audit");
      change("abandoned", row -> row.deferResolution(0));
      var result = relay.runBatch();
      assertEquals(1, result.exhausted());
      assertEquals(1, result.published());
      assertAudit("abandoned", "MAX_FAILURES", 1, true);
    } finally {
      execute("DROP TRIGGER IF EXISTS reject_discard ON \"relay-data\".kc_nats_discard_audit");
      execute("DROP FUNCTION IF EXISTS \"relay-data\".reject_discard()");
    }
    assertEquals(List.of("abandoned", "other", "successor"), storedIds(3));
  }

  @Test
  void offlineExpiryRemovesPayloadAndLeavesOnlyDiagnosticMetadata() throws Exception {
    capture("expired", "offline", new PublicationPolicy(1, 2), 2000);
    final var original = row("expired");
    var docker = broker.getDockerClient();
    docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    try {
      var result = new OutboxRelay(OutboxDiscardIT::transaction, neverPublish(), config).runBatch();
      assertEquals(1, result.expired());
      assertEquals(0, result.published());
      assertNull(row("expired"));
      assertAudit("expired", "EXPIRED", 0, false);
      try (var session = sessions.openSession()) {
        String sql =
            """
            SELECT realm_id, ordering_key, user_sequence, event_type, subject,
                   payload_sha256, created_at, discarded_at, filter_sha256, rule_id,
                   max_age_seconds, max_failures, expires_at
            FROM "relay-data".kc_nats_discard_audit WHERE id='expired'
            """;
        Object[] audit = session.createNativeQuery(sql, Object[].class).getSingleResult();
        assertEquals(original.realmId(), audit[0]);
        assertEquals(original.orderingKey(), audit[1]);
        assertEquals(original.userSequence(), audit[2]);
        assertEquals(original.eventType(), audit[3]);
        assertEquals(original.subject(), audit[4]);
        assertEquals(original.payloadSha256(), audit[5]);
        assertEquals(original.createdAt(), audit[6]);
        assertTrue(((Number) audit[7]).longValue() >= original.expiresAt());
        assertEquals(original.publicationPolicy().filterSha256(), audit[8]);
        assertEquals("discard-rule", audit[9]);
        assertEquals(1, audit[10]);
        assertEquals(2, audit[11]);
        assertEquals(original.expiresAt(), audit[12]);
      }
    } finally {
      docker.startContainerCmd(broker.getContainerId()).exec();
      connectNats();
    }
    assertEquals(0, messages());
    capture("recovered", "offline");
    try (var publisher = new JetStreamPublisher(liveConfig())) {
      assertEquals(
          1,
          new OutboxRelay(OutboxDiscardIT::transaction, publisher, config).runBatch().published());
    }
    assertEquals(List.of("recovered"), storedIds(1));
    assertEquals(1, audits());
  }

  @Test
  void nonHeadExpiryIgnoresBackoffWithoutBypassingProtectedPredecessor() throws Exception {
    capture("protected", "queued", PublicationPolicy.RETRY, 100_000_000);
    capture("expired", "queued", new PublicationPolicy(1, null), 2000);
    capture("successor", "queued");
    capture("other", "other");
    transaction(
        em -> {
          var head = em.find(OutboxEvent.class, "protected");
          for (int i = 0; i < 100; i++) {
            head.recordPublicationFailure(Long.MAX_VALUE, "IOException");
          }
          em.find(OutboxEvent.class, "expired")
              .recordPublicationFailure(Long.MAX_VALUE, "IOException");
          OutboxHeads.refresh(em, head.orderingKey());
        });
    try (var publisher = new JetStreamPublisher(config)) {
      var relay = new OutboxRelay(OutboxDiscardIT::transaction, publisher, config);
      var result = relay.runBatch();
      assertEquals(1, result.expired());
      assertEquals(1, result.published());
      assertNotNull(row("protected"));
      assertFalse(row("successor").publicationMayHaveOccurred());
      assertNull(row("expired"));
      assertEquals(List.of("other"), storedIds(1));
      long headSequence = row("protected").userSequence();
      assertEquals(headSequence + 2, row("successor").userSequence());
      change("protected", row -> row.recordPublicationFailure(0, "due"));
      assertEquals(2, relay.runBatch().published());
    }
    assertEquals(List.of("other", "protected", "successor"), storedIds(3));
    assertEquals(1, audits());
  }

  @ParameterizedTest
  @ValueSource(strings = {"audit", "delete", "commit", "rollback"})
  void rejectedDiscardRetainsOriginalAndBlocksSuccessor(String failure) throws Exception {
    capture("expired", "atomic", new PublicationPolicy(1, null), 2000);
    capture("successor", "atomic");
    rejectDiscard(failure);
    try {
      var result =
          new OutboxRelay(
                  work ->
                      transaction(
                          em -> {
                            work.accept(em);
                            em.flush();
                            if (failure.equals("rollback")) {
                              throw new IllegalStateException("rollback discard");
                            }
                          }),
                  neverPublish(),
                  config)
              .runBatch();
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, result.outcome());
      assertEquals(0, result.discarded());
      assertNotNull(row("expired"));
      assertFalse(row("successor").publicationMayHaveOccurred());
      assertEquals(0, audits());
      assertEquals(0, messages());
    } finally {
      allowDiscard(failure);
    }
    change("expired", row -> row.deferResolution(0));
    try (var publisher = new JetStreamPublisher(config)) {
      var result = new OutboxRelay(OutboxDiscardIT::transaction, publisher, config).runBatch();
      assertEquals(1, result.discarded());
      assertEquals(1, result.published());
    }
    assertEquals(List.of("successor"), storedIds(1));
  }

  @Test
  void uncommittedDiscardCannotReleaseTheHead() throws Exception {
    capture("expired", "uncommitted", new PublicationPolicy(1, null), 2000);
    capture("successor", "uncommitted");
    try (var owner = sessions.openSession()) {
      owner.beginTransaction();
      var head =
          OutboxRepository.lockNextExpired(owner, CaptureRepository.readDatabaseTime(owner))
              .orElseThrow();
      OutboxRepository.discard(
          owner, head, DiscardReason.EXPIRED, CaptureRepository.readDatabaseTime(owner));
      owner.flush();
      assertEquals(
          OutboxRelay.Outcome.NO_WORK,
          new OutboxRelay(OutboxDiscardIT::transaction, neverPublish(), config)
              .runBatch()
              .outcome());
      assertNotNull(row("expired"));
      assertEquals(0, audits());
      owner.getTransaction().rollback();
    }
    assertNotNull(row("expired"));
    assertFalse(row("successor").publicationMayHaveOccurred());
  }

  @Test
  void lostCommitAcknowledgementIsNotCountedAndNextRunRechecksDatabase() throws Exception {
    capture("expired", "commit-unknown", new PublicationPolicy(1, null), 2000);
    capture("successor", "commit-unknown");
    var result =
        new OutboxRelay(
                work -> {
                  transaction(work);
                  throw new IllegalStateException("commit acknowledgement lost");
                },
                neverPublish(),
                config)
            .runBatch();
    assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, result.outcome());
    assertEquals(0, result.discarded());
    assertNull(row("expired"));
    assertEquals(1, audits());
    assertFalse(row("successor").publicationMayHaveOccurred());
    try (var publisher = new JetStreamPublisher(config)) {
      assertEquals(
          1,
          new OutboxRelay(OutboxDiscardIT::transaction, publisher, config).runBatch().published());
    }
  }

  @Test
  void lostPubAckAtFailureLimitDiscardsWithUnknownOutcome() throws Exception {
    capture("abandoned", "lost-ack", new PublicationPolicy(null, 1), 0);
    capture("successor", "lost-ack");
    final var original = row("abandoned");
    try (var actual = new JetStreamPublisher(config)) {
      var relay =
          new OutboxRelay(
              OutboxDiscardIT::transaction,
              publisher(
                  event -> {
                    actual.publish(event);
                    if (event.id().equals("abandoned")) {
                      throw new IOException("ack lost");
                    }
                  }),
              config);
      var result = relay.runBatch();
      assertEquals(1, result.exhausted());
      assertEquals(1, result.published());
      assertEquals(0, result.retries());
    }
    assertAudit("abandoned", "MAX_FAILURES", 1, true);
    assertEquals(List.of("abandoned", "successor"), storedIds(2));
    assertEquals(original.subject(), storedMessages(2).getFirst().getSubject());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void crashAfterIntentOrSendCannotEraseAmbiguity(boolean send) throws Exception {
    capture("abandoned", "crashed", new PublicationPolicy(1, null), 0);
    final var original = row("abandoned");
    var transactions = new AtomicInteger();
    try (var actual = new JetStreamPublisher(config)) {
      var relay =
          new OutboxRelay(
              work -> {
                if (!send) {
                  transaction(work);
                  throw new IllegalStateException("crash after intent commit");
                }
                transaction(
                    em -> {
                      work.accept(em);
                      em.flush();
                      if (transactions.incrementAndGet() == 2) {
                        throw new IllegalStateException("rollback failed attempt");
                      }
                    });
              },
              publisher(
                  event -> {
                    actual.publish(event);
                    throw new IOException("lost ack");
                  }),
              config);
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, relay.runBatch().outcome());
    }
    assertTrue(row("abandoned").publicationMayHaveOccurred());
    assertEquals(0, row("abandoned").attempts());
    awaitExpiry(original);
    change("abandoned", row -> row.deferResolution(0));
    assertEquals(
        1,
        new OutboxRelay(OutboxDiscardIT::transaction, neverPublish(), config)
            .runBatch()
            .discarded());
    assertAudit("abandoned", "EXPIRED", 0, true);
    assertEquals(send ? 1 : 0, messages());
  }

  @Test
  void discardBetweenIntentAndReacquisitionPreventsThePreparedSend() throws Exception {
    capture("abandoned", "between", new PublicationPolicy(1, null), 0);
    final var original = row("abandoned");
    var transactions = new AtomicInteger();
    var result =
        new OutboxRelay(
                work -> {
                  transaction(work);
                  if (transactions.incrementAndGet() == 1) {
                    awaitExpiry(original);
                    assertEquals(
                        1,
                        new OutboxRelay(OutboxDiscardIT::transaction, neverPublish(), config)
                            .runBatch()
                            .discarded());
                  }
                },
                neverPublish(),
                singleRowConfig())
            .runBatch();
    assertEquals(OutboxRelay.Outcome.STALE, result.outcome());
    assertAudit("abandoned", "EXPIRED", 0, true);
    assertEquals(0, messages());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void activeSendIsSkippedAndLateSendAfterOwnershipLossRemainsAmbiguous(boolean loseOwnership)
      throws Exception {
    capture("abandoned", "active", new PublicationPolicy(1, null), 0);
    capture("successor", "active");
    final var original = row("abandoned");
    var backend = new AtomicLong();
    var sending = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var actual = new JetStreamPublisher(config)) {
      var relay =
          new OutboxRelay(
              work ->
                  transaction(
                      em -> {
                        backend.set(
                            ((Number)
                                    em.createNativeQuery("select pg_backend_pid()")
                                        .getSingleResult())
                                .longValue());
                        work.accept(em);
                      }),
              publisher(
                  event -> {
                    sending.countDown();
                    assertTrue(release.await(15, TimeUnit.SECONDS));
                    actual.publish(event);
                  }),
              singleRowConfig());
      var running = executor.submit(relay::runBatch);
      try {
        assertTrue(sending.await(10, TimeUnit.SECONDS));
        awaitExpiry(original);
        assertEquals(
            OutboxRelay.Outcome.NO_WORK,
            new OutboxRelay(OutboxDiscardIT::transaction, neverPublish(), config)
                .runBatch()
                .outcome());
        assertFalse(row("successor").publicationMayHaveOccurred());
        if (loseOwnership) {
          transaction(
              em ->
                  assertEquals(
                      Boolean.TRUE,
                      em.createNativeQuery("select pg_terminate_backend(:pid)", Boolean.class)
                          .setParameter("pid", Math.toIntExact(backend.get()))
                          .getSingleResult()));
          var result = new OutboxRelay(OutboxDiscardIT::transaction, actual, config).runBatch();
          assertEquals(1, result.expired());
          assertEquals(1, result.published());
          assertAudit("abandoned", "EXPIRED", 0, true);
        }
      } finally {
        release.countDown();
      }
      assertEquals(
          loseOwnership ? OutboxRelay.Outcome.TRANSACTION_FAILED : OutboxRelay.Outcome.PUBLISHED,
          running.get(10, TimeUnit.SECONDS).outcome());
    }
    if (loseOwnership) {
      // Database ownership cannot recall a request that is already in flight.
      assertEquals(List.of("successor", "abandoned"), storedIds(2));
    } else {
      assertNull(row("abandoned"));
      assertNotNull(row("successor"));
      assertEquals(0, audits());
      assertEquals(List.of("abandoned"), storedIds(1));
    }
  }

  private static void rejectDiscard(String failure) throws Exception {
    execute(
        "CREATE FUNCTION \"relay-data\".reject_discard() RETURNS trigger LANGUAGE plpgsql"
            + " AS $$ BEGIN RAISE EXCEPTION 'injected discard rejection'; END $$");
    if (!failure.equals("rollback")) {
      String trigger =
          failure.equals("commit")
              ? "CREATE CONSTRAINT TRIGGER reject_discard AFTER INSERT"
              : "CREATE TRIGGER reject_discard BEFORE "
                  + (failure.equals("delete") ? "DELETE" : "INSERT");
      String table = failure.equals("delete") ? "kc_nats_outbox" : "kc_nats_discard_audit";
      String affected = failure.equals("delete") ? "OLD" : "NEW";
      execute(
          trigger
              + " ON \"relay-data\"."
              + table
              + (failure.equals("commit") ? " DEFERRABLE INITIALLY DEFERRED" : "")
              + " FOR EACH ROW WHEN ("
              + affected
              + ".id IN ('expired', 'abandoned'))"
              + " EXECUTE FUNCTION \"relay-data\".reject_discard()");
    }
  }

  private static void allowDiscard(String failure) throws Exception {
    String table = failure.equals("delete") ? "kc_nats_outbox" : "kc_nats_discard_audit";
    execute("DROP TRIGGER IF EXISTS reject_discard ON \"relay-data\"." + table);
    execute("DROP FUNCTION \"relay-data\".reject_discard()");
  }

  private static void capture(String id, String user, PublicationPolicy policy, long ageMillis) {
    transaction(
        em -> {
          var row =
              new OutboxEvent(
                  id,
                  "keycloak.events.relay." + id,
                  "{\"id\":\"" + id + "\",\"private\":\"payload\"}",
                  CaptureRepository.readDatabaseTime(em) - ageMillis,
                  "relay",
                  "io.keycloak.user.login",
                  CaptureRepository.allocateNextSequence(em, "relay", user),
                  new ResolvedPublicationPolicy(
                      policy, EventFilter.digest(new byte[0]), "discard-rule"));
          em.persist(row);
          OutboxHeads.refresh(em, row.orderingKey());
        });
  }

  private static void awaitExpiry(OutboxEvent row) {
    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              try (var session = sessions.openSession()) {
                return CaptureRepository.readDatabaseTime(session) >= row.expiresAt();
              }
            });
  }

  private static EventPublisher neverPublish() {
    return publisher(
        event -> {
          throw new AssertionError("Unexpected publication: " + event.id());
        });
  }

  private static BridgeConfig liveConfig() {
    return BridgeConfig.from(Map.of("nats-url", natsUrl(), "min-replicas", "1"));
  }

  private static BridgeConfig singleRowConfig() {
    return BridgeConfig.from(Map.of("batch-size", "1"));
  }

  private static long audits() throws Exception {
    return scalar("SELECT count(*) FROM \"relay-data\".kc_nats_discard_audit");
  }

  private static void assertAudit(String id, String reason, long attempts, boolean ambiguous)
      throws Exception {
    try (var session = sessions.openSession()) {
      var audit =
          session
              .createQuery(
                  "select a.reason, a.attempts, a.publicationMayHaveOccurred"
                      + " from NatsDiscardAudit a where a.id = :id",
                  Object[].class)
              .setParameter("id", id)
              .getSingleResult();
      assertEquals(DiscardReason.valueOf(reason), audit[0]);
      assertEquals(attempts, audit[1]);
      assertEquals(ambiguous, audit[2]);
    }
  }
}
