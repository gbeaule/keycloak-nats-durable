package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Expiry claims remain bounded when only the opposite event class has a large expired backlog. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OutboxExpiryIT extends RelayIntegrationSupport {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void expiryClaimsSkipOneHundredThousandOppositeClassEvents(boolean orderedBacklog)
      throws Exception {
    capture("template", null);
    populate("expired", 100000, orderedBacklog, true);
    populate("protected", 1000, !orderedBacklog, false);
    execute("DELETE FROM \"relay-data\".kc_nats_outbox WHERE id='template'");
    execute("VACUUM ANALYZE \"relay-data\".kc_nats_outbox");
    execute("VACUUM ANALYZE \"relay-data\".kc_nats_capture_counter");
    var limits =
        BridgeConfig.from(
                Map.of("relay-statement-timeout-ms", "100", "relay-lock-timeout-ms", "50"))
            .relayDatabase();
    var queries = new ArrayList<String>();
    long now = System.currentTimeMillis();
    for (int run = 0; run < 3; run++) {
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
        try {
          RelayDatabase.limitStatements(em, limits);
          assertEquals(
              "100ms",
              em.createNativeQuery("select current_setting('statement_timeout')", String.class)
                  .getSingleResult());
          queries.clear();
          long started = System.nanoTime();
          assertTrue(
              OutboxRepository.lockNextExpired(em, now).orElseThrow().id().startsWith("expired-"));
          em.getTransaction().commit();
          System.out.printf(
              "Indexed expiry claim; ordered_backlog=%s expired=100000 elapsed_ms=%.1f%n",
              orderedBacklog, (System.nanoTime() - started) / 1_000_000.0);
        } finally {
          if (em.getTransaction().isActive()) {
            em.getTransaction().rollback();
          }
        }
      }
    }
    var plans = objectMapper.createArrayNode();
    try (var connection = database.getConnection()) {
      connection.setAutoCommit(false);
      try (var limit = connection.createStatement()) {
        limit.execute("SET LOCAL statement_timeout='100ms'");
      }
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
            String index =
                sql.contains("join")
                    ? "idx_kc_nats_ordered_expiry"
                    : "idx_kc_nats_independent_expiry";
            assertTrue(usesIndex(plan, index), plan::toPrettyString);
          }
        }
      }
      connection.commit();
    }
    assertEquals(2, plans.size(), "Both actual Hibernate expiry queries must use bounded scans");
    Files.writeString(
        Path.of(
            "target/outbox-expiry-" + (orderedBacklog ? "ordered" : "independent") + "-plans.json"),
        plans.toPrettyString());
  }

  private static void populate(String prefix, int count, boolean ordered, boolean expired)
      throws Exception {
    execute(
        """
        INSERT INTO "relay-data".kc_nats_outbox
          (id,subject,payload,realm_id,event_type,payload_sha256,filter_sha256,
           ordering_key,user_sequence,created_at,next_attempt_at,
           max_age_seconds,expires_at,next_expiry_attempt_at)
        SELECT '%s-'||g,h.subject,h.payload,h.realm_id,h.event_type,h.payload_sha256,h.filter_sha256,
          CASE WHEN %s THEN '%s-'||g END,CASE WHEN %s THEN 1 END,h.created_at-2000,h.next_attempt_at,
          CASE WHEN %s THEN 1 END,CASE WHEN %s THEN h.created_at-1000 END,
          CASE WHEN %s THEN h.created_at-1000 END
        FROM "relay-data".kc_nats_outbox h CROSS JOIN generate_series(1,%s) g WHERE h.id='template'
        """
            .formatted(prefix, ordered, prefix, ordered, expired, expired, expired, count));
    if (ordered) {
      execute(
          """
          INSERT INTO "relay-data".kc_nats_capture_counter
            (ordering_key,realm_id,user_id,last_sequence,
             head_event_id,head_next_attempt_at,head_created_at)
          SELECT ordering_key,realm_id,id,user_sequence,id,next_attempt_at,created_at
          FROM "relay-data".kc_nats_outbox WHERE id LIKE '%s-%%'
          """
              .formatted(prefix));
    }
  }

  private static boolean usesIndex(JsonNode plan, String name) {
    if (name.equals(plan.path("Index Name").asText())) {
      return true;
    }
    for (JsonNode child : plan.path("Plans")) {
      if (usesIndex(child, name)) {
        return true;
      }
    }
    return false;
  }

  private static long visitedRows(JsonNode plan) {
    long rows =
        (plan.path("Actual Rows").asLong()
                + plan.path("Rows Removed by Filter").asLong()
                + plan.path("Rows Removed by Join Filter").asLong()
                + plan.path("Rows Removed by Index Recheck").asLong())
            * plan.path("Actual Loops").asLong();
    for (JsonNode child : plan.path("Plans")) {
      rows += visitedRows(child);
    }
    return rows;
  }
}
