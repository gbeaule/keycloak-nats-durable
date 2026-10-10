package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.EntityManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;

/**
 * Recovery and bounded claim work with the production schema, indexes and transaction boundaries.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OutboxHeadIT extends RelayIntegrationSupport {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void concurrentBatchAppendAndLastHeadRemovalPreserveTheNewHead(boolean captureFirst)
      throws Exception {
    String user = captureFirst ? "capture-before-resolution" : "resolution-before-capture";
    String prior = "prior-" + user;
    capture(prior, user);
    final String appended;
    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var first = sessions.openSession()) {
      first.beginTransaction();
      try {
        final long firstBackend =
            first.createNativeQuery("select pg_backend_pid()", Long.class).getSingleResult();
        final String firstAppended;
        if (captureFirst) {
          firstAppended = appendBatch(first, user);
        } else {
          OutboxRepository.remove(first, first.find(OutboxEvent.class, prior));
          firstAppended = null;
        }
        var secondBackend = new CompletableFuture<Long>();
        var second =
            executor.submit(
                () -> {
                  try (var em = sessions.openSession()) {
                    em.beginTransaction();
                    try {
                      // Bound cleanup even if an assertion fails while the first lock is held.
                      RelayDatabase.limitStatements(em, new RelayDatabaseConfig(20, 15000, 15000));
                      secondBackend.complete(
                          em.createNativeQuery("select pg_backend_pid()", Long.class)
                              .getSingleResult());
                      String id = null;
                      if (captureFirst) {
                        OutboxRepository.remove(em, em.find(OutboxEvent.class, prior));
                      } else {
                        id = appendBatch(em, user);
                      }
                      em.getTransaction().commit();
                      return id;
                    } finally {
                      if (em.getTransaction().isActive()) {
                        em.getTransaction().rollback();
                      }
                    }
                  }
                });
        long backend = secondBackend.get(10, TimeUnit.SECONDS);
        await()
            .atMost(Duration.ofSeconds(10))
            .until(
                () ->
                    scalar(
                            "SELECT count(*) FROM pg_stat_activity WHERE pid="
                                + backend
                                + " AND wait_event_type='Lock' AND "
                                + firstBackend
                                + "=ANY(pg_blocking_pids(pid))")
                        == 1);
        assertFalse(second.isDone(), "The second operation must wait for the counter holder");
        first.getTransaction().commit();
        String secondAppended = second.get(10, TimeUnit.SECONDS);
        appended = captureFirst ? firstAppended : secondAppended;
      } finally {
        // Release the holder before ExecutorService.close waits for the blocked worker.
        if (first.getTransaction().isActive()) {
          first.getTransaction().rollback();
        }
      }
    }
    assertNull(row(prior));
    assertEquals(2, row(appended).userSequence());
    assertEquals(
        2,
        scalar(
            "SELECT last_sequence FROM \"relay-data\".kc_nats_capture_counter WHERE user_id='"
                + user
                + "'"));
    assertEquals(
        1,
        scalar(
            "SELECT count(*) FROM \"relay-data\".kc_nats_outbox WHERE ordering_key='"
                + EventOrdering.keyFor("relay", user)
                + "'"));
    assertEquals(
        1,
        scalar(
            "SELECT count(*) FROM \"relay-data\".kc_nats_capture_counter c"
                + " JOIN \"relay-data\".kc_nats_outbox o ON o.id=c.head_event_id"
                + " WHERE c.user_id='"
                + user
                + "' AND o.id='"
                + appended
                + "' AND c.last_sequence=2 AND o.user_sequence=2"
                + " AND c.head_next_attempt_at=o.next_attempt_at"
                + " AND c.head_created_at=o.created_at"));
  }

  @Test
  void atomicBatchAppendPreservesExistingHeadsAndRollsBackBothNewAndExistingQueues()
      throws Exception {
    capture("prior", "batch-existing");
    change("prior", row -> row.deferResolution(Long.MAX_VALUE));
    var encoder = new EventEnvelope(BridgeConfig.from(Map.of()));
    var policy =
        new ResolvedPublicationPolicy(
            PublicationPolicy.RETRY, EventFilter.digest(new byte[0]), null);
    var pending = new ArrayList<CaptureRepository.Capture>();
    for (String user : List.of("batch-new", "batch-existing", "batch-new", "batch-existing")) {
      var event = new Event();
      event.setType(EventType.LOGIN);
      event.setRealmId("relay");
      event.setUserId(user);
      event.setTime(1000);
      pending.add(new CaptureRepository.Capture(encoder.describe(event), policy, encoder));
    }
    transaction(em -> CaptureRepository.persistBatch(em, pending));
    assertEquals(
        3,
        scalar(
            "SELECT last_sequence FROM \"relay-data\".kc_nats_capture_counter"
                + " WHERE user_id='batch-existing'"));
    assertEquals(
        2,
        scalar(
            "SELECT last_sequence FROM \"relay-data\".kc_nats_capture_counter"
                + " WHERE user_id='batch-new'"));
    assertEquals(
        1,
        scalar(
            "SELECT count(*) FROM \"relay-data\".kc_nats_capture_counter"
                + " WHERE user_id='batch-existing' AND head_event_id='prior'"
                + " AND head_next_attempt_at="
                + Long.MAX_VALUE));
    assertEquals(
        1,
        scalar(
            "SELECT count(*) FROM \"relay-data\".kc_nats_capture_counter c"
                + " JOIN \"relay-data\".kc_nats_outbox o ON o.id=c.head_event_id"
                + " WHERE c.user_id='batch-new' AND o.user_sequence=1"));
    assertEquals(
        1,
        scalar(
            "SELECT count(DISTINCT created_at) FROM \"relay-data\".kc_nats_outbox"
                + " WHERE id<>'prior'"));
    assertThrows(
        IllegalStateException.class,
        () ->
            transaction(
                em -> {
                  CaptureRepository.persistBatch(em, pending);
                  throw new IllegalStateException("injected batch rollback");
                }));
    assertEquals(5, scalar("SELECT count(*) FROM \"relay-data\".kc_nats_outbox"));
    assertEquals(
        3,
        scalar(
            "SELECT last_sequence FROM \"relay-data\".kc_nats_capture_counter"
                + " WHERE user_id='batch-existing'"));
    assertEquals(
        2,
        scalar(
            "SELECT last_sequence FROM \"relay-data\".kc_nats_capture_counter"
                + " WHERE user_id='batch-new'"));
  }

  @Test
  void failedRemovalDefersItsRowAndLetsOtherUsersProgress() throws Exception {
    capture("a1", "a");
    capture("a2", "a");
    capture("b1", "b");
    execute(
        """
        CREATE FUNCTION "relay-data".reject_head_delete() RETURNS trigger LANGUAGE plpgsql AS $body$
        BEGIN
          IF OLD.id = 'a1' THEN RAISE EXCEPTION 'injected deletion failure'; END IF;
          RETURN OLD;
        END $body$
        """);
    execute(
        "CREATE TRIGGER reject_head_delete BEFORE DELETE ON \"relay-data\".kc_nats_outbox"
            + " FOR EACH ROW EXECUTE FUNCTION \"relay-data\".reject_head_delete()");
    var sent = new ArrayList<String>();
    var relay =
        new OutboxRelay(OutboxHeadIT::transaction, publisher(row -> sent.add(row.id())), config);
    try {
      assertEquals(OutboxRelay.Outcome.TRANSACTION_FAILED, relay.runBatch().outcome());
      assertEquals(0, row("a1").attempts());
      assertTrue(row("a1").nextAttemptAt() > System.currentTimeMillis());
      assertEquals(1, relay.runBatch().published());
      assertEquals(List.of("a1", "b1"), sent);
      assertFalse(row("a2").publicationMayHaveOccurred());
    } finally {
      execute("DROP TRIGGER reject_head_delete ON \"relay-data\".kc_nats_outbox");
      execute("DROP FUNCTION \"relay-data\".reject_head_delete()");
    }
    change("a1", row -> row.deferResolution(0));
    assertEquals(2, relay.runBatch().published());
    assertEquals(List.of("a1", "b1", "a1", "a2"), sent);
  }

  @Test
  void explicitHeadUpdatesPreserveSequenceAllocationAndRollBackWithEventChanges() throws Exception {
    capture("first", "progress");
    capture("second", "progress");
    final String original = row("first").payload();
    final long sequence = row("second").userSequence();
    transaction(
        em -> {
          // Cache a counter snapshot before native sequence allocation and explicit head writes.
          em.find(CaptureCounter.class, row("first").orderingKey());
          capture(em, "third", "relay", "progress");
          em.flush();
        });
    assertEquals(sequence + 1, row("third").userSequence());
    assertEquals(
        sequence + 1,
        scalar(
            "SELECT last_sequence FROM \"relay-data\".kc_nats_capture_counter"
                + " WHERE user_id='progress'"));
    assertThrows(
        IllegalStateException.class,
        () ->
            transaction(
                em -> {
                  OutboxRepository.remove(em, em.find(OutboxEvent.class, "first"));
                  em.flush();
                  throw new IllegalStateException("injected rollback");
                }));
    assertEquals(
        1,
        scalar(
            "SELECT count(*) FROM \"relay-data\".kc_nats_capture_counter"
                + " WHERE head_event_id='first'"));
    assertEquals(original, row("first").payload());
    assertEquals(sequence, row("second").userSequence());
    var sent = new ArrayList<String>();
    assertEquals(
        3,
        new OutboxRelay(OutboxHeadIT::transaction, publisher(row -> sent.add(row.id())), config)
            .runBatch()
            .published());
    assertEquals(List.of("first", "second", "third"), sent);
    assertEquals(
        0,
        scalar(
            "SELECT count(*) FROM \"relay-data\".kc_nats_capture_counter"
                + " WHERE user_id='progress' AND head_event_id IS NOT NULL"));
    capture("fourth", "progress");
    assertEquals(sequence + 2, row("fourth").userSequence());
  }

  @Test
  void claimingUnrelatedWorkDoesNotScanOneHundredThousandBlockedSuccessors() throws Exception {
    capture("head", "hot");
    change("head", row -> row.recordPublicationFailure(Long.MAX_VALUE, "delayed"));
    execute(
        """
        INSERT INTO "relay-data".kc_nats_outbox
          (id,version,subject,payload,realm_id,event_type,payload_sha256,filter_sha256,
           ordering_key,user_sequence,publication_may_have_occurred,created_at,next_attempt_at,attempts)
        SELECT lpad(g::text,36,'0'),0,h.subject,h.payload,h.realm_id,h.event_type,h.payload_sha256,
          h.filter_sha256,h.ordering_key,h.user_sequence+g,false,h.created_at,0,0
        FROM "relay-data".kc_nats_outbox h CROSS JOIN generate_series(1,100000) g WHERE h.id='head'
        """);
    execute(
        "UPDATE \"relay-data\".kc_nats_capture_counter SET last_sequence=last_sequence+100000"
            + " WHERE user_id='hot'");
    capture("other-user", "cold");
    capture("independent", null);
    execute("VACUUM ANALYZE \"relay-data\".kc_nats_outbox");
    execute("ANALYZE \"relay-data\".kc_nats_capture_counter");
    var queries = new ArrayList<String>();
    long now = System.currentTimeMillis();
    for (int run = 0; run < 3; run++) {
      queries.clear();
      try (var em =
          sessions
              .withOptions()
              .statementInspector(
                  (org.hibernate.resource.jdbc.spi.StatementInspector)
                      sql -> {
                        queries.add(sql);
                        return sql;
                      })
              .openSession()) {
        em.beginTransaction();
        long started = System.nanoTime();
        String id = OutboxRepository.lockNextDue(em, now).orElseThrow().id();
        assertTrue(id.equals("other-user") || id.equals("independent"));
        em.getTransaction().commit();
        System.out.printf(
            "Indexed head claim; blocked=100000 elapsed_ms=%.1f%n",
            (System.nanoTime() - started) / 1_000_000.0);
      }
    }
    var plans = objectMapper.createArrayNode();
    try (var connection = database.getConnection()) {
      for (String sql : queries) {
        if (!sql.contains("order by")) {
          continue;
        }
        try (var statement =
            connection.prepareStatement("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql)) {
          statement.setLong(1, now);
          statement.setInt(2, 1);
          try (var result = statement.executeQuery()) {
            assertTrue(result.next());
            JsonNode plan = objectMapper.readTree(result.getString(1)).get(0).path("Plan");
            plans.add(plan);
            assertTrue(visitedRows(plan) < 100, plan::toPrettyString);
          }
        }
      }
    }
    assertEquals(2, plans.size(), "Both ordered and independent work use bounded index scans");
    Files.writeString(Path.of("target/outbox-head-plans.json"), plans.toPrettyString());
  }

  private static String appendBatch(EntityManager em, String user) {
    final var encoder = new EventEnvelope(BridgeConfig.from(Map.of()));
    var event = new Event();
    event.setType(EventType.LOGIN);
    event.setRealmId("relay");
    event.setUserId(user);
    event.setTime(1000);
    var policy =
        new ResolvedPublicationPolicy(
            PublicationPolicy.RETRY, EventFilter.digest(new byte[0]), null);
    CaptureRepository.persistBatch(
        em, List.of(new CaptureRepository.Capture(encoder.describe(event), policy, encoder)));
    return em.createQuery(
            "from NatsOutboxEvent where orderingKey=:key and userSequence=2", OutboxEvent.class)
        .setParameter("key", EventOrdering.keyFor("relay", user))
        .getSingleResult()
        .id();
  }

  private static long visitedRows(JsonNode plan) {
    long rows =
        (plan.path("Actual Rows").asLong() + plan.path("Rows Removed by Filter").asLong())
            * plan.path("Actual Loops").asLong();
    for (JsonNode child : plan.path("Plans")) {
      rows += visitedRows(child);
    }
    return rows;
  }
}
