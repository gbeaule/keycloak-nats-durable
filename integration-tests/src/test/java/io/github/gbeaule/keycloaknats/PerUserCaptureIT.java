package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.BooleanNode;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;

/**
 * Real PostgreSQL locks and listener callbacks, plus the supported runtime's emitted admin paths.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class PerUserCaptureIT extends IntegrationSupport {
  private static SessionFactory sessions;

  @BeforeAll
  static void start() throws Exception {
    startInfrastructure();
    drained();
    broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    sessions =
        new Configuration()
            .addAnnotatedClass(OutboxEvent.class)
            .addAnnotatedClass(CaptureCounter.class)
            .setProperty("hibernate.connection.url", postgres.getJdbcUrl())
            .setProperty("hibernate.connection.username", postgres.getUsername())
            .setProperty("hibernate.connection.password", postgres.getPassword())
            .setProperty("hibernate.hbm2ddl.auto", "validate")
            .buildSessionFactory();
  }

  @AfterAll
  static void stop() throws Exception {
    if (sessions != null) {
      sessions.close();
    }
    stopInfrastructure();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void simultaneousFirstCapturesHandleBothUniqueIndexesAndRollback(boolean rollBackSome)
      throws Exception {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int round = 0; round < 10; round++) {
        String user = UUID.randomUUID().toString();
        var ready = new CountDownLatch(8);
        var start = new CountDownLatch(1);
        var captures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        for (int worker = 0; worker < 8; worker++) {
          boolean rollback = rollBackSome && worker % 2 == 0;
          captures.add(
              executor.submit(
                  () -> {
                    try (var session = sessions.openSession()) {
                      session.beginTransaction();
                      ready.countDown();
                      assertTrue(start.await(10, TimeUnit.SECONDS));
                      listener(session, EventFilter::all).onEvent(event("first-capture", user));
                      if (rollback) {
                        session.getTransaction().rollback();
                      } else {
                        session.getTransaction().commit();
                      }
                    }
                    return null;
                  }));
        }
        try {
          assertTrue(ready.await(10, TimeUnit.SECONDS));
        } finally {
          start.countDown();
        }
        for (var capture : captures) {
          capture.get(10, TimeUnit.SECONDS);
        }
        assertEquals(
            java.util.stream.LongStream.rangeClosed(1, rollBackSome ? 4 : 8).boxed().toList(),
            sequences(user));
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "false,false",
    "true,false",
    "false,true",
    "true,true"
  })
  void captureCannotCommitAheadOfTheLockHolder(boolean commit, boolean existing) throws Exception {
    String user = UUID.randomUUID().toString();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var first = sessions.openSession()) {
      first.beginTransaction();
      if (existing) {
        listener(first, EventFilter::all).onEvent(event("capture", user));
        first.getTransaction().commit();
        first.beginTransaction();
      }
      captureNow(first, event("capture", user));
      var pid = new CompletableFuture<Long>();
      var second =
          executor.submit(
              () -> {
                try (var session = sessions.openSession()) {
                  session.beginTransaction();
                  pid.complete(
                      ((Number)
                              session
                                  .createNativeQuery("select pg_backend_pid()", Long.class)
                                  .getSingleResult())
                          .longValue());
                  listener(session, EventFilter::all).onEvent(event("capture", user));
                  session.getTransaction().commit();
                }
              });
      long backend = pid.get(10, TimeUnit.SECONDS);
      try {
        await()
            .atMost(Duration.ofSeconds(10))
            .until(
                () ->
                    scalar(
                            "SELECT count(*) FROM pg_stat_activity WHERE pid = "
                                + backend
                                + " AND wait_event_type = 'Lock'")
                        == 1);
        assertFalse(second.isDone(), "The later callback already tried to reach commit");
        assertEquals(existing ? 1 : 0, count(user));
      } finally {
        if (commit) {
          first.getTransaction().commit();
        } else {
          first.getTransaction().rollback();
        }
      }
      second.get(10, TimeUnit.SECONDS);
      int total = 1 + (commit ? 1 : 0) + (existing ? 1 : 0);
      assertEquals(
          java.util.stream.LongStream.rangeClosed(1, total).boxed().toList(), sequences(user));
    }
  }

  @Test
  void callbackOrderAndIndependentUsersRealmsAndUserlessEvents() throws Exception {
    String user = UUID.randomUUID().toString();
    try (var first = sessions.openSession();
        var other = sessions.openSession()) {
      first.beginTransaction();
      var listener = listener(first, EventFilter::all);
      var login = event("realm-a", user);
      login.setId("first-callback");
      listener.onEvent(login);
      login.setId("second-callback");
      listener.onEvent(login);
      other.beginTransaction();
      other.createNativeMutationQuery("SET LOCAL lock_timeout = '1s'").executeUpdate();
      var independent = listener(other, EventFilter::all);
      independent.onEvent(event("realm-b", user));
      independent.onEvent(event("realm-a", user + "-other"));
      independent.onEvent(event("independent", null));
      independent.onEvent(event("independent", " "));
      other.getTransaction().commit();
      assertEquals(1, count(user));
      first.getTransaction().commit();
    }
    assertEquals(List.of(1L, 1L, 2L), sequences(user));
    try (var session = sessions.openSession()) {
      assertEquals(
          List.of("first-callback", "second-callback"),
          session
              .createNativeQuery(
                  "SELECT payload::jsonb->'data'->>'keycloakEventId' FROM kc_nats_outbox"
                      + " WHERE ordering_key=:key ORDER BY user_sequence",
                  String.class)
              .setParameter("key", new EventOrdering("realm-a", user, 1).key())
              .getResultList());
    }
    assertEquals(
        0, scalar("SELECT count(*) FROM kc_nats_capture_counter WHERE realm_id='independent'"));
    assertEquals(
        2,
        scalar(
            "SELECT count(*) FROM kc_nats_outbox WHERE realm_id='independent'"
                + " AND ordering_key IS NULL AND user_sequence IS NULL"));
  }

  @Test
  void frozenPolicySurvivesReloadWhileWaitingAndExclusionsAllocateNothing() throws Exception {
    String user = UUID.randomUUID().toString();
    var initial =
        EventFilter.parse(
            """
            {"userEvents":["LOGIN"],"adminEvents":[],"delivery":{"rules":[
              {"id":"short","match":{"kind":"user","eventTypes":["LOGIN"]},
               "policy":{"action":"discard","maxAgeSeconds":60,"maxFailures":2}},
              {"id":"overlap","match":{"kind":"user","eventTypes":["*"]},
               "policy":{"action":"retry"}}]}}
            """
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
    var current = new AtomicReference<>(initial);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var holder = sessions.openSession()) {
      holder.beginTransaction();
      captureNow(holder, event("capture", user));
      var pid = new CompletableFuture<Long>();
      var staged = new CountDownLatch(1);
      var reloaded = new CountDownLatch(1);
      var pending =
          executor.submit(
              () -> {
                try (var session = sessions.openSession()) {
                  session.beginTransaction();
                  pid.complete(
                      ((Number)
                              session
                                  .createNativeQuery("select pg_backend_pid()", Long.class)
                                  .getSingleResult())
                          .longValue());
                  var capture = listener(session, current::get);
                  capture.onEvent(event("capture", user));
                  staged.countDown();
                  assertTrue(reloaded.await(10, TimeUnit.SECONDS));
                  capture.onEvent(
                      event("capture", user)); // The replacement excludes this callback.
                  session.getTransaction().commit();
                }
                return null;
              });
      long backend = pid.get(10, TimeUnit.SECONDS);
      try {
        assertTrue(staged.await(10, TimeUnit.SECONDS));
        current.set(
            EventFilter.parse(
                "{\"userEvents\":[],\"adminEvents\":[]}"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        reloaded.countDown();
        await()
            .atMost(Duration.ofSeconds(10))
            .until(
                () ->
                    scalar(
                            "SELECT count(*) FROM pg_stat_activity WHERE pid="
                                + backend
                                + " AND wait_event_type='Lock'")
                        == 1);
        current.set(
            EventFilter.parse(
                "{\"userEvents\":[],\"adminEvents\":[]}"
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      } finally {
        reloaded.countDown();
        holder.getTransaction().rollback();
      }
      pending.get(10, TimeUnit.SECONDS);
    }
    assertEquals(List.of(1L), sequences(user));
    try (var session = sessions.openSession()) {
      var row =
          session
              .createQuery("from NatsOutboxEvent where orderingKey=:key", OutboxEvent.class)
              .setParameter("key", new EventOrdering("capture", user, 1).key())
              .getSingleResult();
      assertEquals(
          initial.resolve(event("capture", user), row.subject()).orElseThrow(),
          row.publicationPolicy());
      assertEquals("short", row.publicationPolicy().ruleId());
      assertEquals(new PublicationPolicy(60, 2), row.publicationPolicy().policy());
      assertEquals(row.createdAt() + 60_000, row.expiresAt());
      assertFalse(row.payload().contains("maxAgeSeconds"));
      assertEquals(
          "1", objectMapper.readTree(row.payload()).at("/data/ordering/sequence").asText());
      long now = CaptureRepository.readDatabaseTime(session);
      assertTrue(row.createdAt() <= now && row.createdAt() > now - 60_000);
    }
  }

  @Test
  void databaseRejectionRollsBackAllCallbacksAndSequenceOverflowCannotWrap() {
    String user = UUID.randomUUID().toString();
    try (var session = sessions.openSession()) {
      session.beginTransaction();
      var capture = listener(session, EventFilter::all);
      captureNow(session, event("capture", user));
      session
          .createNativeMutationQuery(
              "UPDATE kc_nats_capture_counter SET last_sequence="
                  + Long.MAX_VALUE
                  + " WHERE user_id=:user")
          .setParameter("user", user)
          .executeUpdate();
      capture.onEvent(event("capture", user));
      assertThrows(RuntimeException.class, () -> session.getTransaction().commit());
    }
    commitEvent(event("capture", user));
    assertEquals(List.of(1L), sequences(user));
  }

  @Test
  void lockTimeoutRollsBackEarlierCallbacksForOtherUsers() {
    String locked = "z-" + UUID.randomUUID();
    String earlier = "a-" + UUID.randomUUID();
    try (var holder = sessions.openSession();
        var victim = sessions.openSession()) {
      holder.beginTransaction();
      captureNow(holder, event("capture", locked));
      victim.beginTransaction();
      var capture = listener(victim, EventFilter::all);
      capture.onEvent(event("capture", earlier));
      victim.createNativeMutationQuery("SET LOCAL lock_timeout='250ms'").executeUpdate();
      capture.onEvent(event("capture", locked));
      assertThrows(RuntimeException.class, () -> victim.getTransaction().commit());
      holder.getTransaction().rollback();
      assertEquals(0, count(earlier));
    }
    commitEvent(event("capture", earlier));
    assertEquals(List.of(1L), sequences(earlier));
  }

  @Test
  void opposingCallbackOrdersPrepareInOneLockOrderWithoutDeadlock() throws Exception {
    String realm = "ordered-locks-" + UUID.randomUUID();
    var users = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    var staged = new CountDownLatch(2);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var outcomes = new ArrayList<Future<?>>();
      for (int index = 0; index < 2; index++) {
        int side = index;
        outcomes.add(
            executor.submit(
                () -> {
                  try (var session = sessions.openSession()) {
                    session.beginTransaction();
                    session
                        .createNativeMutationQuery("SET LOCAL lock_timeout='5s'")
                        .executeUpdate();
                    var capture = listener(session, EventFilter::all);
                    capture.onEvent(event(realm, users.get(side)));
                    capture.onEvent(event(realm, users.get(1 - side)));
                    staged.countDown();
                    assertTrue(staged.await(10, TimeUnit.SECONDS));
                    session.getTransaction().commit();
                  }
                  return null;
                }));
      }
      for (var outcome : outcomes) {
        outcome.get(15, TimeUnit.SECONDS);
      }
    }
    assertEquals(List.of(1L, 2L), sequences(users.get(0)));
    assertEquals(List.of(1L, 2L), sequences(users.get(1)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "user_sequence=0",
        "user_sequence=NULL",
        "ordering_key=NULL",
        "attempts=-1",
        "max_age_seconds=0",
        "max_age_seconds=1",
        "max_failures=1000001"
      })
  void databaseRejectsInvalidCaptureState(String assignment) throws Exception {
    String user = UUID.randomUUID().toString();
    commitEvent(event("capture", user));
    try (var connection = database.getConnection();
        var update =
            connection.prepareStatement(
                "UPDATE kc_nats_outbox SET " + assignment + " WHERE ordering_key=?")) {
      update.setString(1, new EventOrdering("capture", user, 1).key());
      assertEquals(
          "23514", assertThrows(java.sql.SQLException.class, update::executeUpdate).getSQLState());
    }
    assertEquals(List.of(1L), sequences(user));
  }

  @Test
  void databaseEnforcesUniqueUserTuplesAndOutboxPositions() throws Exception {
    String user = UUID.randomUUID().toString();
    try (var session = sessions.openSession()) {
      session.beginTransaction();
      var capture = listener(session, EventFilter::all);
      capture.onEvent(event("capture", user));
      capture.onEvent(event("capture", user + "-other"));
      session.getTransaction().commit();
    }
    try (var connection = database.getConnection();
        var duplicate =
            connection.prepareStatement(
                "INSERT INTO kc_nats_capture_counter(ordering_key,realm_id,user_id,last_sequence)"
                    + " VALUES ('another-key','capture',?,1)")) {
      duplicate.setString(1, user);
      assertEquals(
          "23505",
          assertThrows(java.sql.SQLException.class, duplicate::executeUpdate).getSQLState());
    }
    try (var connection = database.getConnection();
        var duplicate =
            connection.prepareStatement(
                "UPDATE kc_nats_outbox SET ordering_key=? WHERE ordering_key=?")) {
      duplicate.setString(1, new EventOrdering("capture", user, 1).key());
      duplicate.setString(2, new EventOrdering("capture", user + "-other", 1).key());
      assertEquals(
          "23505",
          assertThrows(java.sql.SQLException.class, duplicate::executeUpdate).getSQLState());
    }
  }

  @Test
  void nestedRuntimeEventsShareTheTargetCounterAndDeletionDoesNotResetIt() throws Exception {
    String user = createUser();
    assertEquals(
        204,
        request(
                "PUT",
                "/admin/realms/durable-test/users/" + user + "/reset-password",
                Map.of("type", "password", "value", "integration-password", "temporary", false))
            .statusCode());
    var roles =
        objectMapper.readTree(request("GET", "/admin/realms/durable-test/roles", null).body());
    var role =
        java.util.stream.StreamSupport.stream(roles.spliterator(), false)
            .filter(r -> r.path("name").asText().startsWith("default-roles-"))
            .findFirst()
            .orElseThrow();
    assertEquals(
        204,
        request(
                "DELETE",
                "/admin/realms/durable-test/users/" + user + "/role-mappings/realm",
                List.of(role))
            .statusCode());
    var credentials =
        objectMapper.readTree(
            request("GET", "/admin/realms/durable-test/users/" + user + "/credentials", null)
                .body());
    assertEquals(
        204,
        request(
                "DELETE",
                "/admin/realms/durable-test/users/"
                    + user
                    + "/credentials/"
                    + credentials.get(0).path("id").asText(),
                null)
            .statusCode());
    assertEquals(
        204, request("DELETE", "/admin/realms/durable-test/users/" + user, null).statusCode());
    assertEquals(List.of(1L, 2L, 3L, 4L, 5L), sequences(user));
    try (var session = sessions.openSession()) {
      session.beginTransaction();
      String realm =
          session
              .createNativeQuery("SELECT id FROM realm WHERE name='durable-test'", String.class)
              .getSingleResult();
      String key = new EventOrdering(realm, user, 1).key();
      var rows =
          session
              .createQuery("from NatsOutboxEvent where orderingKey=:key", OutboxEvent.class)
              .setParameter("key", key)
              .setLockMode(jakarta.persistence.LockModeType.PESSIMISTIC_WRITE)
              .getResultList();
      assertEquals(5, rows.size());
      for (var row : rows) {
        var data = objectMapper.readTree(row.payload()).path("data");
        assertEquals(user, data.path("userId").asText());
        assertNotEquals(user, data.path("actorUserId").asText());
        session.remove(row);
      }
      session.getTransaction().commit();
      session.beginTransaction();
      listener(session, EventFilter::all).onEvent(event(realm, user));
      session.getTransaction().commit();
    }
    assertEquals(List.of(6L), sequences(user));
  }

  @ParameterizedTest
  @ValueSource(strings = {"/credentials", "/consents", "/%2F+用户/credentials"})
  void routedUserIdsRemainIntactAcrossActionsUpdatesAndDeletion(String suffix) throws Exception {
    String prefix = UUID.randomUUID().toString().substring(0, 8);
    String user = prefix + suffix;
    // Seed an opaque ID directly because the create-user API generates UUIDs for local users.
    try (var connection = database.getConnection();
        var insert =
            connection.prepareStatement(
                "INSERT INTO user_entity"
                    + " (id,realm_id,username,enabled,email_verified,not_before,created_timestamp)"
                    + " SELECT ?,id,?,true,false,0,0 FROM realm WHERE name='durable-test'")) {
      insert.setString(1, user);
      insert.setString(2, "routed-" + prefix);
      assertEquals(1, insert.executeUpdate());
    }
    String path =
        "/admin/realms/durable-test/users/"
            + URLEncoder.encode(user, StandardCharsets.UTF_8).replace("+", "%20");
    var reset =
        request(
            "PUT",
            path + "/reset-password",
            Map.of("type", "password", "value", "integration-password", "temporary", false));
    assertEquals(204, reset.statusCode(), reset.body());
    var update = request("PUT", path, Map.of("enabled", false));
    assertEquals(204, update.statusCode(), update.body());
    var credentials = objectMapper.readTree(request("GET", path + "/credentials", null).body());
    assertEquals(
        204,
        request("DELETE", path + "/credentials/" + credentials.get(0).path("id").asText(), null)
            .statusCode());
    assertEquals(204, request("DELETE", path, null).statusCode());
    assertEquals(List.of(1L, 2L, 3L, 4L), sequences(user));
    assertEquals(List.of(), sequences(prefix));
    try (var session = sessions.openSession()) {
      var rows =
          session
              .createQuery(
                  "from NatsOutboxEvent where orderingKey in"
                      + " (select c.orderingKey from NatsCaptureCounter c where c.userId=:user)",
                  OutboxEvent.class)
              .setParameter("user", user)
              .getResultList();
      assertEquals(4, rows.size());
      for (var row : rows) {
        var data = objectMapper.readTree(row.payload()).path("data");
        assertEquals(user, data.path("userId").asText());
        assertNotEquals(user, data.path("actorUserId").asText());
        if (data.path("operationType").asText().equals("UPDATE")) {
          assertEquals(BooleanNode.FALSE, data.get("userEnabled"));
        } else {
          assertFalse(data.has("userEnabled"));
        }
      }
    }
  }

  private static Event event(String realm, String user) {
    var event = new Event();
    event.setRealmId(realm);
    event.setUserId(user);
    event.setType(EventType.LOGIN);
    event.setTime(1); // Capture time must come from PostgreSQL, not this timestamp.
    return event;
  }

  private static void commitEvent(Event event) {
    try (var session = sessions.openSession()) {
      session.beginTransaction();
      listener(session, EventFilter::all).onEvent(event);
      session.getTransaction().commit();
    }
  }

  private static DurableEventListener listener(Session em, Supplier<EventFilter> filter) {
    var session = mock(KeycloakSession.class);
    var jpa = mock(JpaConnectionProvider.class);
    var tx = mock(KeycloakTransactionManager.class);
    when(session.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
    when(jpa.getEntityManager()).thenReturn(em);
    when(session.getTransactionManager()).thenReturn(tx);
    when(tx.isActive()).thenAnswer(call -> em.getTransaction().isActive());
    doAnswer(
            call -> {
              KeycloakTransaction preparation = call.getArgument(0);
              preparation.begin();
              em.getTransaction()
                  .registerSynchronization(
                      new Synchronization() {
                        @Override
                        public void beforeCompletion() {
                          preparation.commit();
                        }

                        @Override
                        public void afterCompletion(int status) {}
                      });
              return null;
            })
        .when(tx)
        .enlistPrepare(org.mockito.ArgumentMatchers.any());
    doAnswer(
            call -> {
              KeycloakTransaction completion = call.getArgument(0);
              completion.begin();
              em.getTransaction()
                  .registerSynchronization(
                      new Synchronization() {
                        @Override
                        public void beforeCompletion() {}

                        @Override
                        public void afterCompletion(int status) {
                          if (status == Status.STATUS_COMMITTED) {
                            completion.commit();
                          } else {
                            completion.rollback();
                          }
                        }
                      });
              return null;
            })
        .when(tx)
        .enlistAfterCompletion(org.mockito.ArgumentMatchers.any());
    doAnswer(
            call -> {
              em.getTransaction().markRollbackOnly();
              return null;
            })
        .when(tx)
        .setRollbackOnly();
    return new DurableEventListener(session, BridgeConfig.from(Map.of()), () -> {}, filter);
  }

  private static void captureNow(Session em, Event event) {
    var envelope = new EventEnvelope(BridgeConfig.from(Map.of()));
    var row =
        envelope.serialize(
            UUID.randomUUID().toString(),
            envelope.describe(event),
            CaptureRepository.allocateNextSequence(em, event.getRealmId(), event.getUserId()),
            CaptureRepository.readDatabaseTime(em),
            new ResolvedPublicationPolicy(
                PublicationPolicy.RETRY, EventFilter.all().sha256(), null));
    em.persist(row);
    OutboxHeads.refresh(em, row.orderingKey());
  }

  private static List<Long> sequences(String user) {
    try (var session = sessions.openSession()) {
      return session
          .createNativeQuery(
              "SELECT user_sequence FROM kc_nats_outbox WHERE ordering_key IN"
                  + " (SELECT ordering_key FROM kc_nats_capture_counter WHERE user_id=:user)"
                  + " ORDER BY user_sequence",
              Long.class)
          .setParameter("user", user)
          .getResultList();
    }
  }

  private static long count(String user) {
    return sequences(user).size();
  }
}
