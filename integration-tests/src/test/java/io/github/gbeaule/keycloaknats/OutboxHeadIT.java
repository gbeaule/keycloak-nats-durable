package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Recovery and bounded claim work with the production schema, indexes and transaction boundaries.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OutboxHeadIT extends RelayIntegrationSupport {
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
    change("head", row -> row.failed(Long.MAX_VALUE, "delayed"));
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
