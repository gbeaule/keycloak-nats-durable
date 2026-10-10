package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.BooleanNode;
import io.opentelemetry.api.trace.Span;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.common.ClientConnection;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import org.keycloak.models.AbstractKeycloakTransaction;
import org.keycloak.models.ClientModel;
import org.keycloak.models.ClientProvider;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserProvider;
import org.keycloak.services.DefaultKeycloakTransactionManager;
import org.keycloak.services.resources.admin.AdminAuth;
import org.keycloak.services.resources.admin.AdminEventBuilder;
import org.keycloak.tracing.TracingProvider;

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
            .setProperty("hibernate.jpa.compliance.transaction", "true")
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
          int callbackCount = worker % 3 + 1;
          captures.add(
              executor.submit(
                  () -> {
                    try (var session = sessions.openSession()) {
                      session.beginTransaction();
                      ready.countDown();
                      assertTrue(start.await(10, TimeUnit.SECONDS));
                      var capture = listener(session, EventFilter::all);
                      for (int callback = 0; callback < callbackCount; callback++) {
                        capture.onEvent(event("first-capture", user));
                      }
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
        long expected =
            java.util.stream.IntStream.range(0, 8)
                .filter(worker -> !rollBackSome || worker % 2 != 0)
                .map(worker -> worker % 3 + 1)
                .sum();
        assertEquals(
            java.util.stream.LongStream.rangeClosed(1, expected).boxed().toList(), sequences(user));
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
  void interleavedListenerInstancesShareCallbackOrderAndFreezeTheirSnapshots() throws Exception {
    String user = UUID.randomUUID().toString();
    String realm = UUID.randomUUID().toString();
    try (var em = sessions.openSession()) {
      em.beginTransaction();
      var owner = captureSession(em);
      var first = listener(owner, EventFilter::all);
      final var second = listener(owner, EventFilter::all);
      var login = event(realm, user);
      login.setId("first-callback");
      first.onEvent(login);
      var admin = new AdminEvent();
      admin.setId("second-callback");
      admin.setRealmId(realm);
      admin.setResourceType(ResourceType.USER);
      admin.setOperationType(OperationType.DELETE);
      admin.setResourcePath("users/" + user);
      second.onEvent(admin, false);
      login.setId("third-callback");
      first.onEvent(login);
      admin.setId("mutated-after-callback");
      em.getTransaction().commit();
      assertEquals(
          List.of("first-callback", "second-callback", "third-callback"),
          em.createNativeQuery(
                  "SELECT payload::jsonb->'data'->>'keycloakEventId' FROM kc_nats_outbox"
                      + " WHERE ordering_key=:key ORDER BY user_sequence",
                  String.class)
              .setParameter("key", EventOrdering.keyFor(realm, user))
              .getResultList());
    }
    assertEquals(List.of(1L, 2L, 3L), sequences(user));
  }

  @ParameterizedTest
  @ValueSource(strings = {"callback", "laterPrepare", "firstPrepare"})
  @SuppressWarnings("unchecked")
  void swallowedAdminLookupFailureCannotCommitTheAccountUpdate(String phase) throws Exception {
    String userId = createUser();
    var positionsBefore = sequences(userId);
    var failure = new IllegalStateException("remote user storage failed");
    try (var em = sessions.openSession()) {
      em.beginTransaction();
      final String realmId =
          em.createNativeQuery("SELECT realm_id FROM user_entity WHERE id=:user", String.class)
              .setParameter("user", userId)
              .getSingleResult();
      assertEquals(
          1,
          em.createNativeMutationQuery("UPDATE user_entity SET enabled=false WHERE id=:user")
              .setParameter("user", userId)
              .executeUpdate());
      var owner = mock(KeycloakSession.class);
      var attributes = new HashMap<String, Object>();
      when(owner.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
      doAnswer(call -> attributes.put(call.getArgument(0), call.getArgument(1)))
          .when(owner)
          .setAttribute(anyString(), any());
      var tx = new DefaultKeycloakTransactionManager(owner);
      tx.setJTAPolicy(KeycloakTransactionManager.JTAPolicy.NOT_SUPPORTED);
      when(owner.getTransactionManager()).thenReturn(tx);
      // The real manager owns account work before the builder dispatches its listener callback.
      tx.enlist(
          new AbstractKeycloakTransaction() {
            @Override
            protected void commitImpl() {
              em.getTransaction().commit();
            }

            @Override
            protected void rollbackImpl() {
              em.getTransaction().rollback();
            }
          });
      var tracing = mock(TracingProvider.class);
      doAnswer(
              call -> {
                ((Consumer<?>) call.getArgument(2)).accept(null);
                return null;
              })
          .when(tracing)
          .trace(any(Class.class), any(String.class), any(Consumer.class));
      when(tracing.getCurrentSpan()).thenReturn(mock(Span.class));
      when(owner.getProvider(TracingProvider.class)).thenReturn(tracing);
      var jpa = mock(JpaConnectionProvider.class);
      when(owner.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
      when(jpa.getEntityManager()).thenReturn(em);
      var realm = mock(RealmModel.class);
      when(realm.getId()).thenReturn(realmId);
      when(realm.getName()).thenReturn("durable-test");
      when(realm.getEventsListenersStream()).thenAnswer(call -> Stream.of("nats-durable"));
      var realms = mock(RealmProvider.class);
      when(owner.realms()).thenReturn(realms);
      when(owner.clients()).thenReturn(mock(ClientProvider.class));
      when(realms.getRealm(realmId)).thenReturn(realm);
      var users = mock(UserProvider.class);
      var user = mock(UserModel.class);
      when(owner.users()).thenReturn(users);
      when(users.getUserById(realm, userId)).thenReturn(user);
      when(user.isEnabled()).thenThrow(failure);
      var context = mock(KeycloakContext.class);
      when(context.getRealm()).thenReturn(realm);
      when(owner.getContext()).thenReturn(context);
      var factory = mock(KeycloakSessionFactory.class);
      when(owner.getKeycloakSessionFactory()).thenReturn(factory);
      var provider = mock(EventListenerProviderFactory.class);
      var wakeup = mock(Runnable.class);
      when(provider.getId()).thenReturn("nats-durable");
      when(provider.create(owner))
          .thenAnswer(call -> new DurableEventListener(owner, BridgeConfig.from(Map.of()), wakeup));
      when(factory.getProviderFactoriesStream(EventListenerProvider.class))
          .thenAnswer(call -> Stream.of(provider));
      var auth = mock(AdminAuth.class);
      var actor = mock(UserModel.class);
      when(actor.getId()).thenReturn("actor");
      var client = mock(ClientModel.class);
      when(client.getId()).thenReturn("internal-client");
      when(auth.getRealm()).thenReturn(realm);
      when(auth.getUser()).thenReturn(actor);
      when(auth.getClient()).thenReturn(client);
      tx.begin();
      Runnable captureAdmin =
          () ->
              new AdminEventBuilder(realm, auth, owner, mock(ClientConnection.class))
                  .resource(ResourceType.USER)
                  .operation(OperationType.UPDATE)
                  .resourcePath("users", userId)
                  .success();
      if (!phase.equals("callback")) {
        if (phase.equals("laterPrepare")) {
          new DurableEventListener(owner, BridgeConfig.from(Map.of()), wakeup)
              .onEvent(event(realmId, userId));
        }
        tx.enlistPrepare(
            new AbstractKeycloakTransaction() {
              @Override
              protected void commitImpl() {
                captureAdmin.run();
              }

              @Override
              protected void rollbackImpl() {}
            });
      } else {
        captureAdmin.run();
      }
      var rejection = assertThrows(RuntimeException.class, tx::commit);
      if (phase.equals("callback")) {
        assertSame(failure, rejection.getCause());
      }
      assertTrue(tx.getRollbackOnly());
      assertFalse(em.getTransaction().isActive());
      verifyNoInteractions(wakeup);
    }
    assertEquals(
        1, scalar("SELECT count(*) FROM user_entity WHERE id='" + userId + "' AND enabled"));
    assertEquals(positionsBefore, sequences(userId));
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
  void atomicRangeCanReachTheLastSequenceButCannotPartiallyAllocatePastIt() {
    String user = "z-" + UUID.randomUUID();
    final String earlier = "a-" + UUID.randomUUID();
    commitEvent(event("capture", user));
    try (var session = sessions.openSession()) {
      session.beginTransaction();
      session
          .createNativeMutationQuery(
              "UPDATE kc_nats_capture_counter SET last_sequence="
                  + (Long.MAX_VALUE - 3)
                  + " WHERE user_id=:user")
          .setParameter("user", user)
          .executeUpdate();
      var capture = listener(session, EventFilter::all);
      for (int callback = 0; callback < 3; callback++) {
        capture.onEvent(event("capture", user));
      }
      session.getTransaction().commit();
    }
    var expected = List.of(1L, Long.MAX_VALUE - 2, Long.MAX_VALUE - 1, Long.MAX_VALUE);
    assertEquals(expected, sequences(user));
    try (var session = sessions.openSession()) {
      session.beginTransaction();
      var capture = listener(session, EventFilter::all);
      capture.onEvent(event("capture", earlier));
      capture.onEvent(event("capture", user));
      capture.onEvent(event("capture", user));
      assertThrows(RuntimeException.class, () -> session.getTransaction().commit());
    }
    assertEquals(expected, sequences(user));
    assertEquals(List.of(), sequences(earlier));
    commitEvent(event("capture", earlier));
    assertEquals(List.of(1L), sequences(earlier));
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
                    var owner = captureSession(session);
                    listener(owner, EventFilter::all).onEvent(event(realm, users.get(side)));
                    listener(owner, EventFilter::all).onEvent(event(realm, users.get(1 - side)));
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void atomicAllocationCannotReuseACounterWithAnInconsistentIdentity(boolean wrongKey)
      throws Exception {
    String user = UUID.randomUUID().toString();
    commitEvent(event("capture", user));
    try (var connection = database.getConnection();
        var corrupt =
            connection.prepareStatement(
                "UPDATE kc_nats_capture_counter SET "
                    + (wrongKey ? "ordering_key='incorrect-key'" : "realm_id='incorrect-realm'")
                    + " WHERE user_id=?")) {
      corrupt.setString(1, user);
      assertEquals(1, corrupt.executeUpdate());
    }
    try (var session = sessions.openSession()) {
      session.beginTransaction();
      var capture = listener(session, EventFilter::all);
      capture.onEvent(event("capture", user));
      capture.onEvent(event("capture", user));
      assertThrows(RuntimeException.class, () -> session.getTransaction().commit());
    }
    assertEquals(
        1,
        scalar("SELECT last_sequence FROM kc_nats_capture_counter WHERE user_id='" + user + "'"));
    assertEquals(
        1,
        scalar(
            "SELECT count(*) FROM kc_nats_outbox WHERE realm_id='capture'"
                + " AND payload::jsonb->'data'->>'userId'='"
                + user
                + "'"));
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
    return listener(captureSession(em), filter);
  }

  private static DurableEventListener listener(
      KeycloakSession owner, Supplier<EventFilter> filter) {
    return new DurableEventListener(owner, BridgeConfig.from(Map.of()), () -> {}, filter);
  }

  private static KeycloakSession captureSession(Session em) {
    var session = mock(KeycloakSession.class);
    var attributes = new HashMap<String, Object>();
    when(session.getAttribute(anyString())).thenAnswer(call -> attributes.get(call.getArgument(0)));
    doAnswer(call -> attributes.put(call.getArgument(0), call.getArgument(1)))
        .when(session)
        .setAttribute(anyString(), any());
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
    return session;
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
