package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Verifies that account writes precede capture counters in real concurrent account transactions.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class CaptureLockOrderIT extends IntegrationSupport {
  private static final Map<String, Object> evidence = new LinkedHashMap<>();
  private static String realmId;

  @BeforeAll
  static void start() throws Exception {
    startInfrastructure(System.getProperty("keycloak.version", "26.8.1"), container -> {}, false);
    realmId =
        objectMapper
            .readTree(request("GET", "/admin/realms/durable-test", null).body())
            .path("id")
            .asText();
  }

  @AfterAll
  static void stop() throws Exception {
    try {
      writeEvidence();
      if (keycloak != null && keycloak.getContainerId() != null) {
        Files.writeString(Path.of("target/capture-lock-order-keycloak.log"), keycloak.getLogs());
      }
      if (postgres != null && postgres.getContainerId() != null) {
        Files.writeString(Path.of("target/capture-lock-order-postgres.log"), postgres.getLogs());
      }
    } finally {
      stopInfrastructure();
    }
  }

  @Test
  @Order(1)
  void profileFlushHoldsTheUserRowBeforeWaitingForItsCaptureCounter() throws Exception {
    String user = createUser();
    drained();
    loginAdmin();
    String endpoint = "/admin/realms/durable-test/users/" + user;
    String orderingKey = EventOrdering.keyFor(realmId, user);
    var collected = new LinkedHashMap<String, Object>();
    evidence.put("coordinated", collected);
    String postgresBefore = postgres.getLogs();
    // The inner finally releases our counter even on assertion/timeout failure, before executor
    // close joins either HTTP task. Otherwise the fixture itself could keep both requests blocked.
    try (var coordinator = database.getConnection();
        var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      coordinator.setAutoCommit(false);
      try {
        int coordinatorPid = backendPid(coordinator);
        collected.put("coordinatorPid", coordinatorPid);
        try (var lock =
            coordinator.prepareStatement(
                "SELECT ordering_key FROM kc_nats_capture_counter"
                    + " WHERE ordering_key=? FOR UPDATE")) {
          lock.setString(1, orderingKey);
          try (var found = lock.executeQuery()) {
            assertTrue(found.next(), "The created user must have a capture counter");
          }
        }
        var update =
            executor.submit(
                () ->
                    request(
                        "PUT",
                        endpoint,
                        Map.of("enabled", false, "firstName", "profile-" + UUID.randomUUID())));
        var updatePid = new CompletableFuture<Integer>();
        await("PUT waits for its counter after flushing the account write")
            .atMost(Duration.ofSeconds(8))
            .pollInterval(Duration.ofMillis(20))
            .until(
                () -> {
                  var waiting = counterWaiters();
                  if (waiting.size() == 1) {
                    updatePid.complete(waiting.getFirst());
                    return true;
                  }
                  assertFalse(update.isDone(), "PUT completed before waiting for the counter");
                  return false;
                });
        int putPid = updatePid.get();
        assertTrue(blockedBy(putPid, coordinatorPid));
        collected.put("putPid", putPid);
        collected.put("putQuery", activeQuery(putPid));

        var deletion = executor.submit(() -> request("DELETE", endpoint, null));
        var deletePid = new CompletableFuture<Integer>();
        await("DELETE waits for PUT's already-flushed account row")
            .atMost(Duration.ofSeconds(8))
            .pollInterval(Duration.ofMillis(20))
            .until(
                () -> {
                  var waiting = userRowWaiters(putPid);
                  if (waiting.size() == 1) {
                    deletePid.complete(waiting.getFirst());
                    return true;
                  }
                  assertFalse(
                      deletion.isDone(), "DELETE completed before waiting for the user row");
                  return false;
                });
        int removalPid = deletePid.get();
        String removalQuery = activeQuery(removalPid);
        assertTrue(removalQuery.toLowerCase(Locale.ROOT).contains("user_entity"));
        assertTrue(blockedBy(removalPid, putPid));
        assertTrue(blockedBy(putPid, coordinatorPid));
        assertFalse(blockedBy(putPid, removalPid), "The requests must not form a lock cycle");
        assertEquals(
            List.of(putPid), counterWaiters(), "DELETE must wait on the account row first");
        collected.put("deletePid", removalPid);
        collected.put("deleteQuery", removalQuery);
        collected.put("verifiedAccountBeforeCounterGraph", blockingGraph());
        writeEvidence();

        coordinator.commit();
        var updated = update.get(30, TimeUnit.SECONDS);
        var removed = deletion.get(30, TimeUnit.SECONDS);
        collected.put("putStatus", updated.statusCode());
        collected.put("deleteStatus", removed.statusCode());
        assertEquals(204, updated.statusCode(), updated.body());
        assertEquals(204, removed.statusCode(), removed.body());
        assertNoNewDeadlock(postgresBefore);
      } finally {
        coordinator.rollback();
        writeEvidence();
      }
    }
    drained();
  }

  @Test
  @Order(2)
  void deletionQueuedBeforeProfileFlushRollsBackTheUpdate() throws Exception {
    String user = createUser();
    drained();
    final long sequenceBefore = captureSequence(user);
    String endpoint = "/admin/realms/durable-test/users/" + user;
    var collected = new LinkedHashMap<String, Object>();
    evidence.put("deletionBeforeProfileFlush", collected);
    String postgresBefore = postgres.getLogs();
    final String keycloakBefore = keycloak.getLogs();
    try (var coordinator = database.getConnection();
        var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      coordinator.setAutoCommit(false);
      try {
        int coordinatorPid = backendPid(coordinator);
        try (var lock =
            coordinator.prepareStatement("SELECT id FROM user_entity WHERE id=? FOR UPDATE")) {
          lock.setString(1, user);
          try (var found = lock.executeQuery()) {
            assertTrue(found.next());
          }
        }
        var deletion = executor.submit(() -> request("DELETE", endpoint, null));
        var deletePid = new CompletableFuture<Integer>();
        await("DELETE queues behind the held user row")
            .atMost(Duration.ofSeconds(8))
            .pollInterval(Duration.ofMillis(20))
            .until(
                () -> {
                  var waiting = userRowWaiters(coordinatorPid);
                  if (waiting.size() == 1) {
                    deletePid.complete(waiting.getFirst());
                    return true;
                  }
                  assertFalse(
                      deletion.isDone(), "DELETE completed before waiting for the user row");
                  return false;
                });
        int removalPid = deletePid.get();
        var update =
            executor.submit(
                () ->
                    request(
                        "PUT", endpoint, Map.of("enabled", false, "firstName", "racing-profile")));
        await("PUT has loaded the user and queues its flush behind DELETE")
            .atMost(Duration.ofSeconds(8))
            .pollInterval(Duration.ofMillis(20))
            .until(
                () -> {
                  if (userRowWaiters(removalPid).size() == 1) {
                    return true;
                  }
                  assertFalse(update.isDone(), "PUT completed before waiting for the user row");
                  return false;
                });
        collected.put("blockingGraph", blockingGraph());
        coordinator.commit();
        var removed = deletion.get(30, TimeUnit.SECONDS);
        var updated = update.get(30, TimeUnit.SECONDS);
        collected.put("deleteStatus", removed.statusCode());
        collected.put("putStatus", updated.statusCode());
        collected.put("putBody", updated.body());
        writeEvidence();
        assertEquals(204, removed.statusCode(), removed.body());
        assertNoNewDeadlock(postgresBefore);
        assertDeletedUserConflict(updated, user, keycloakBefore);
        assertEquals(404, request("GET", endpoint, null).statusCode());
        // Only deletion commits an event; the rejected profile write must not allocate a position.
        assertEquals(sequenceBefore + 1, captureSequence(user));
      } finally {
        coordinator.rollback();
        writeEvidence();
      }
    }
    drained();
  }

  @Test
  @Order(3)
  void uncoordinatedProfileDeletionAndPasswordChangesCompleteWithoutDeadlocks() throws Exception {
    var rounds = new ArrayList<Map<String, Object>>();
    evidence.put("uncoordinated", rounds);
    String postgresBefore = postgres.getLogs();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int round = 0; round < 20; round++) {
        if (round % 10 == 0) {
          loginAdmin();
        }
        String user = createUser();
        String endpoint = "/admin/realms/durable-test/users/" + user;
        drained();
        final long sequenceBefore = captureSequence(user);
        final String keycloakBefore = keycloak.getLogs();
        var start = new CountDownLatch(1);
        var update =
            executor.submit(
                () -> {
                  start.await();
                  return request(
                      "PUT",
                      endpoint,
                      Map.of("enabled", false, "firstName", "profile-" + UUID.randomUUID()));
                });
        boolean delete = round % 2 == 0;
        var mixed =
            executor.submit(
                () -> {
                  start.await();
                  return delete
                      ? request("DELETE", endpoint, null)
                      : request(
                          "PUT",
                          endpoint + "/reset-password",
                          Map.of(
                              "type",
                              "password",
                              "value",
                              "lock-order-password",
                              "temporary",
                              false));
                });
        start.countDown();
        var updated = update.get(30, TimeUnit.SECONDS);
        var other = mixed.get(30, TimeUnit.SECONDS);
        rounds.add(
            Map.of(
                "round",
                round,
                "kind",
                delete ? "profile-and-delete" : "profile-and-password-reset",
                "user",
                user,
                "putStatus",
                updated.statusCode(),
                "putBody",
                updated.body(),
                "mixedStatus",
                other.statusCode(),
                "mixedBody",
                other.body()));
        writeEvidence();
        assertEquals(204, other.statusCode(), other.body());
        assertNoNewDeadlock(postgresBefore);
        // Deletion can win before lookup (404) or after lookup but before flush (stale entity).
        if (updated.statusCode() != 204) {
          assertTrue(delete, updated.body());
          if (updated.statusCode() != 404) {
            assertDeletedUserConflict(updated, user, keycloakBefore);
          }
          assertEquals(sequenceBefore + 1, captureSequence(user));
        }
        if (delete) {
          assertEquals(404, request("GET", endpoint, null).statusCode());
        }
      }
    }
    assertEquals(20, rounds.size());
    assertNoNewDeadlock(postgresBefore);
    drained();
  }

  private static void assertDeletedUserConflict(
      HttpResponse<String> update, String user, String keycloakBefore) throws Exception {
    assertEquals(500, update.statusCode(), update.body());
    assertEquals(
        "Database operation failed",
        objectMapper.readTree(update.body()).path("errorMessage").asText());
    String after = keycloak.getLogs();
    String added = after.substring(Math.min(keycloakBefore.length(), after.length()));
    assertTrue(
        added
            .lines()
            .anyMatch(
                line ->
                    line.contains("org.hibernate.StaleObjectStateException:")
                        && line.contains("org.keycloak.models.jpa.entities.UserEntity")
                        && line.contains(user)),
        added);
  }

  private static long captureSequence(String user) throws SQLException {
    try (var db = database.getConnection();
        var query =
            db.prepareStatement(
                "SELECT last_sequence FROM kc_nats_capture_counter WHERE user_id=?")) {
      query.setString(1, user);
      try (var rows = query.executeQuery()) {
        assertTrue(rows.next());
        return rows.getLong(1);
      }
    }
  }

  private static int backendPid(Connection db) throws SQLException {
    try (var statement = db.createStatement();
        var rows = statement.executeQuery("SELECT pg_backend_pid()")) {
      assertTrue(rows.next());
      return rows.getInt(1);
    }
  }

  private static List<Integer> counterWaiters() throws SQLException {
    var pids = new ArrayList<Integer>();
    try (var db = database.getConnection();
        var statement =
            db.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE pid<>pg_backend_pid()"
                    + " AND datname=current_database() AND wait_event_type='Lock'"
                    + " AND query ILIKE '%kc_nats_capture_counter%'"
                    + " AND query ILIKE '%SET LAST_SEQUENCE%'"
                    + " AND cardinality(pg_blocking_pids(pid))>0 ORDER BY query_start")) {
      try (var rows = statement.executeQuery()) {
        while (rows.next()) {
          pids.add(rows.getInt(1));
        }
      }
    }
    return pids;
  }

  private static List<Integer> userRowWaiters(int owner) throws SQLException {
    var pids = new ArrayList<Integer>();
    try (var db = database.getConnection();
        var statement =
            db.prepareStatement(
                "SELECT pid FROM pg_stat_activity WHERE pid<>pg_backend_pid()"
                    + " AND datname=current_database() AND wait_event_type='Lock'"
                    + " AND query ILIKE '%user_entity%' AND ?=ANY(pg_blocking_pids(pid))")) {
      statement.setInt(1, owner);
      try (var rows = statement.executeQuery()) {
        while (rows.next()) {
          pids.add(rows.getInt(1));
        }
      }
    }
    return pids;
  }

  private static String activeQuery(int pid) throws SQLException {
    try (var db = database.getConnection();
        var statement = db.prepareStatement("SELECT query FROM pg_stat_activity WHERE pid=?")) {
      statement.setInt(1, pid);
      try (var rows = statement.executeQuery()) {
        assertTrue(rows.next());
        return rows.getString(1);
      }
    }
  }

  private static boolean blockedBy(int waiter, int owner) throws SQLException {
    try (var db = database.getConnection();
        var statement = db.prepareStatement("SELECT ?=ANY(pg_blocking_pids(?))")) {
      statement.setInt(1, owner);
      statement.setInt(2, waiter);
      try (var rows = statement.executeQuery()) {
        return rows.next() && rows.getBoolean(1);
      }
    }
  }

  private static List<Map<String, Object>> blockingGraph() throws SQLException {
    var graph = new ArrayList<Map<String, Object>>();
    try (var db = database.getConnection();
        var statement = db.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT pid,query,pg_blocking_pids(pid)::text FROM pg_stat_activity"
                    + " WHERE datname=current_database() AND pid<>pg_backend_pid()"
                    + " AND cardinality(pg_blocking_pids(pid))>0 ORDER BY pid")) {
      while (rows.next()) {
        graph.add(
            Map.of(
                "pid", rows.getInt(1),
                "query", rows.getString(2),
                "blockingPids", rows.getString(3)));
      }
    }
    return graph;
  }

  private static void assertNoNewDeadlock(String before) {
    String after = postgres.getLogs();
    String added = after.substring(Math.min(before.length(), after.length()));
    assertFalse(added.contains("deadlock detected"), added);
    assertFalse(added.contains("40P01"), added);
  }

  private static void writeEvidence() throws Exception {
    Files.writeString(
        Path.of("target/capture-lock-order-evidence.json"),
        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(evidence));
  }
}
