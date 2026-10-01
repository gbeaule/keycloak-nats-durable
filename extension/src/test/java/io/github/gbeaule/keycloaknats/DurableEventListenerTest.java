package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakTransaction;
import org.keycloak.models.KeycloakTransactionManager;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserProvider;
import org.mockito.ArgumentCaptor;

class DurableEventListenerTest {
  KeycloakSession session;
  EntityManager em;
  KeycloakTransactionManager tx;
  DurableEventListener listener;
  Runnable wakeRelay;
  org.mockito.MockedStatic<CaptureRepository> capture;
  org.mockito.MockedStatic<OutboxHeads> heads;

  @BeforeEach
  void setup() {
    capture = org.mockito.Mockito.mockStatic(CaptureRepository.class);
    heads = org.mockito.Mockito.mockStatic(OutboxHeads.class);
    capture
        .when(() -> CaptureRepository.next(any(), any(), any()))
        .thenAnswer(
            call ->
                call.getArgument(2) == null
                    ? null
                    : new EventOrdering(call.getArgument(1), call.getArgument(2), 1));
    capture.when(() -> CaptureRepository.databaseTime(any())).thenReturn(1234L);
    session = mock(KeycloakSession.class);
    em = mock(EntityManager.class);
    tx = mock(KeycloakTransactionManager.class);
    when(session.getTransactionManager()).thenReturn(tx);
    when(tx.isActive()).thenReturn(true);
    var jpa = mock(JpaConnectionProvider.class);
    when(session.getProvider(JpaConnectionProvider.class)).thenReturn(jpa);
    when(jpa.getEntityManager()).thenReturn(em);
    wakeRelay = mock(Runnable.class);
    listener = new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay);
  }

  @AfterEach
  void closeCapture() {
    capture.close();
    heads.close();
  }

  @Test
  void excludedEventsDoNotTouchDatabaseOrObserveUsers() throws Exception {
    var none = EventFilterTest.parse("{\"userEvents\":[],\"adminEvents\":[]}");
    listener =
        new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay, () -> none);
    listener.onEvent(EventEnvelopeTest.login());
    listener.onEvent(EventEnvelopeTest.admin(OperationType.UPDATE), false);
    verifyNoInteractions(session, em, tx, wakeRelay);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"userEvents\":[],\"adminEvents\":[]}",
        """
        {"subjects":["other.>"],"userEvents":["*"],"adminEvents":[
          {"resourceType":"*","operations":["*"]}]}
        """
      })
  void excludedOversizeSubjectDoesNotTouchDatabaseOrMarkRollback(String json) throws Exception {
    var policy = EventFilterTest.parse(json);
    var config = BridgeConfig.from(Map.of("subject-prefix", "p".repeat(128)));
    listener = new DurableEventListener(session, config, wakeRelay, () -> policy);
    var event = EventEnvelopeTest.admin(OperationType.CREATE);
    event.setRealmId("12345678-1234-1234-1234-123456789012");
    event.setResourceTypeAsString("x".repeat(255));
    event.setResourcePath("widgets/example");
    listener.onEvent(event, false);
    capture.verifyNoInteractions();
    verifyNoInteractions(session, em, tx, wakeRelay);
  }

  @Test
  void selectedOversizeSubjectMarksRollbackWithoutPersisting() {
    var config = BridgeConfig.from(Map.of("subject-prefix", "p".repeat(128)));
    listener = new DurableEventListener(session, config, wakeRelay);
    var event = EventEnvelopeTest.admin(OperationType.CREATE);
    event.setRealmId("12345678-1234-1234-1234-123456789012");
    event.setResourceTypeAsString("x".repeat(255));
    event.setResourcePath("widgets/example");
    var failure =
        assertThrows(IllegalArgumentException.class, () -> listener.onEvent(event, false));
    assertEquals("Event subject exceeds storage limit", failure.getMessage());
    verify(tx).setRollbackOnly();
    verify(tx, never()).enlistAfterCompletion(any());
    capture.verifyNoInteractions();
    verifyNoInteractions(em, wakeRelay);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"realmIds\":[\"other-realm\"]",
        "\"subjects\":[\"other.>\"]",
        "\"clientIds\":[\"other-client\"]"
      })
  void independentScopeExclusionsAvoidPersistenceAndUserLookups(String scope) throws Exception {
    var policy =
        EventFilterTest.parse(
            "{"
                + scope
                + ",\"userEvents\":[\"*\"],\"adminEvents\":["
                + "{\"resourceType\":\"*\",\"operations\":[\"*\"]}]}");
    listener =
        new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay, () -> policy);
    listener.onEvent(EventEnvelopeTest.login());
    listener.onEvent(EventEnvelopeTest.admin(OperationType.UPDATE), false);
    verifyNoInteractions(session, em, tx, wakeRelay);
  }

  @Test
  void snapshotsImmediatelyAndPersistsInPrepareBeforeTheRequestCommits() throws Exception {
    var event = EventEnvelopeTest.login();
    event.setSessionId("login-session");
    listener.onEvent(event);
    verifyNoInteractions(em);
    capture.verifyNoInteractions();
    event.setSessionId("changed-after-callback");
    prepare();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    var order = inOrder(tx, em);
    order.verify(tx).isActive();
    order.verify(tx).enlistPrepare(any());
    order.verify(tx).enlistAfterCompletion(any());
    order.verify(em).persist(captured.capture());
    order.verify(em).flush();
    verifyNoMoreInteractions(tx, em);
    verifyNoInteractions(wakeRelay);
    var json = new ObjectMapper().readTree(captured.getValue().payload());
    assertEquals(event.getId(), json.at("/data/keycloakEventId").textValue());
    assertEquals(event.getUserId(), json.at("/data/userId").textValue());
    assertEquals("login-session", json.at("/data/sessionId").textValue());
    assertEquals(captured.getValue().id(), json.get("id").textValue());
  }

  @Test
  void persistenceFailureExplicitlyMarksRollbackOnly() {
    var failure = new IllegalStateException("disk-full");
    doThrow(failure).when(em).persist(any());
    listener.onEvent(EventEnvelopeTest.login());
    assertSame(failure, assertThrows(IllegalStateException.class, this::prepare));
    verify(tx).setRollbackOnly();
    verify(em, never()).flush();
    verifyNoInteractions(wakeRelay);
  }

  @Test
  void serializationFailureAlsoMarksRollbackOnly() {
    var event = EventEnvelopeTest.login();
    event.setType(null);
    assertThrows(NullPointerException.class, () -> listener.onEvent(event));
    verify(tx).setRollbackOnly();
    verifyNoInteractions(em, wakeRelay);
    verify(tx, never()).enlistAfterCompletion(any());
  }

  @Test
  void flushFailureInPreparePreventsCommit() {
    doThrow(new IllegalStateException("constraint failure")).when(em).flush();
    listener.onEvent(EventEnvelopeTest.login());
    assertThrows(IllegalStateException.class, this::prepare);
    verify(tx).setRollbackOnly();
    verifyNoInteractions(wakeRelay);
  }

  @Test
  void inactiveTransactionFailsClosed() {
    when(tx.isActive()).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
    verify(em, never()).persist(any());
    verify(tx).setRollbackOnly();
    verifyNoInteractions(em, wakeRelay);
    verify(tx, never()).enlistAfterCompletion(any());
  }

  @Test
  void deletingDoesNotReadDeletedUser() {
    listener.onEvent(EventEnvelopeTest.admin(OperationType.DELETE), false);
    prepare();
    verify(session, never()).users();
    verify(em).persist(any());
  }

  @Test
  void actionDoesNotClaimAnEnablementObservation() {
    listener.onEvent(EventEnvelopeTest.admin(OperationType.ACTION), false);
    prepare();
    verify(session, never()).users();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    assertFalse(captured.getValue().payload().contains("userEnabled"));
  }

  @Test
  void coalescesEventsAndWakesOnlyAfterSuccessfulCommit() {
    listener.onEvent(EventEnvelopeTest.login());
    listener.onEvent(EventEnvelopeTest.login());
    verifyNoInteractions(wakeRelay);
    prepare();
    verify(em, times(2)).persist(any(OutboxEvent.class));
    verify(em).flush();
    var callback = ArgumentCaptor.forClass(KeycloakTransaction.class);
    verify(tx).enlistAfterCompletion(callback.capture());
    callback.getValue().begin();
    callback.getValue().commit();
    verify(wakeRelay).run();
  }

  @Test
  void rolledBackTransactionDoesNotWakeRelay() {
    listener.onEvent(EventEnvelopeTest.login());
    var callback = ArgumentCaptor.forClass(KeycloakTransaction.class);
    verify(tx).enlistAfterCompletion(callback.capture());
    callback.getValue().begin();
    callback.getValue().rollback();
    verifyNoInteractions(wakeRelay);
    verifyNoInteractions(em);
    capture.verifyNoInteractions();
  }

  @Test
  void fatalCaptureFailuresAlsoMarkRollbackAndNeverNotify() {
    var failure = new AssertionError("fatal persistence failure");
    doThrow(failure).when(em).persist(any());
    listener.onEvent(EventEnvelopeTest.login());
    assertSame(failure, assertThrows(IllegalStateException.class, this::prepare).getCause());
    verify(tx).setRollbackOnly();
    verify(em, never()).flush();
    verifyNoInteractions(wakeRelay);
  }

  @Test
  void statePredicateRejectsEnabledAccountsAfterLookupWithoutEnteringTheOutbox() throws Exception {
    final var policy =
        EventFilterTest.parse(
            """
            {"userEvents":[],"adminEvents":[
              {"resourceType":"USER","operations":["UPDATE"],"userEnabled":false}]}
            """);
    var realms = mock(RealmProvider.class);
    var users = mock(UserProvider.class);
    var realm = mock(RealmModel.class);
    var user = mock(UserModel.class);
    when(session.realms()).thenReturn(realms);
    when(session.users()).thenReturn(users);
    when(realms.getRealm("realm")).thenReturn(realm);
    when(users.getUserById(realm, "target")).thenReturn(user);
    when(user.isEnabled()).thenReturn(true);
    listener =
        new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay, () -> policy);
    listener.onEvent(EventEnvelopeTest.admin(OperationType.UPDATE), true);
    verify(user).isEnabled();
    verifyNoInteractions(em, tx, wakeRelay);
    verify(session, never()).getProvider(JpaConnectionProvider.class);
  }

  @Test
  void callbackUsesOneSnapshotEvenWhenObservationTriggersReload() throws Exception {
    var initial =
        EventFilterTest.parse(
            DeliveryRulesTest.document(
                DeliveryRulesTest.rule(
                    "disabled",
                    DeliveryRulesTest.ADMIN_MATCH.replace("}", ",\"userEnabled\":false}"),
                    "{\"action\":\"discard\",\"maxFailures\":2}")));
    var replacement = EventFilterTest.parse("{\"userEvents\":[],\"adminEvents\":[]}");
    var current = new AtomicReference<>(initial);
    @SuppressWarnings("unchecked")
    Supplier<EventFilter> source = mock(Supplier.class);
    when(source.get()).thenAnswer(call -> current.get());
    var realms = mock(RealmProvider.class);
    var realm = mock(RealmModel.class);
    var users = mock(UserProvider.class);
    var user = mock(UserModel.class);
    when(session.realms()).thenReturn(realms);
    when(realms.getRealm("realm")).thenReturn(realm);
    when(session.users()).thenReturn(users);
    when(users.getUserById(realm, "target")).thenReturn(user);
    when(user.isEnabled())
        .thenAnswer(
            call -> {
              current.set(replacement);
              return false;
            });
    listener = new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay, source);
    listener.onEvent(EventEnvelopeTest.admin(OperationType.UPDATE), false);
    verify(source).get();
    prepare();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    assertEquals(
        new ResolvedPublicationPolicy(new PublicationPolicy(null, 2), initial.sha256(), "disabled"),
        captured.getValue().publicationPolicy());
    listener.onEvent(EventEnvelopeTest.login());
    verify(source, times(2)).get();
    verify(em).persist(any(OutboxEvent.class));
  }

  @Test
  void notificationEnlistmentFailureMarksTheRequestForRollback() {
    var failure = new IllegalStateException("completion enlistment failed");
    doThrow(failure).when(tx).enlistAfterCompletion(any());
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login())));
    verifyNoInteractions(em);
    verify(tx).setRollbackOnly();
    verifyNoInteractions(wakeRelay);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void recordsObservedEnablementForDirectSuccessfulUserUpdates(boolean enabled) throws Exception {
    var realms = mock(RealmProvider.class);
    var users = mock(UserProvider.class);
    var realm = mock(RealmModel.class);
    var user = mock(UserModel.class);
    when(session.realms()).thenReturn(realms);
    when(session.users()).thenReturn(users);
    when(realms.getRealm("realm")).thenReturn(realm);
    when(users.getUserById(realm, "target")).thenReturn(user);
    when(user.isEnabled()).thenReturn(enabled);
    var event = EventEnvelopeTest.admin(OperationType.UPDATE);
    listener.onEvent(event, true);
    prepare();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    var data = new ObjectMapper().readTree(captured.getValue().payload()).get("data");
    assertEquals(Boolean.toString(enabled), data.get("userEnabled").toString());
    assertEquals("target", data.get("userId").textValue());
    assertFalse(data.has("representation"));
    assertFalse(captured.getValue().payload().contains("sensitive-password"));
    verify(users).getUserById(realm, "target");
    verify(tx, never()).setRollbackOnly();
  }

  @Test
  void routedIdentityIsSharedByStateFilteringPolicyEnvelopeAndOrdering() throws Exception {
    String id = "f:provider:opaque/credentials";
    var event = EventEnvelopeTest.admin(OperationType.UPDATE);
    event.setResourcePath("users/" + id);
    var request = AdminRequestFixtures.request(event, id, Map.of());
    var context = request.getContext();
    when(session.getContext()).thenReturn(context);
    var parameters = request.getContext().getUri().getPathParameters();
    var realms = mock(RealmProvider.class);
    var users = mock(UserProvider.class);
    var realm = mock(RealmModel.class);
    var user = mock(UserModel.class);
    when(session.realms()).thenReturn(realms);
    when(session.users()).thenReturn(users);
    when(realms.getRealm("realm")).thenReturn(realm);
    when(users.getUserById(realm, id)).thenReturn(user);
    when(user.isEnabled())
        .thenAnswer(
            call -> {
              parameters.putSingle("user-id", "different-later-context");
              return false;
            });
    var filter =
        EventFilterTest.parse(
            """
            {"userEvents":[],"adminEvents":[
              {"resourceType":"USER","operations":["UPDATE"],"userEnabled":false}],
             "delivery":{"rules":[{"id":"disabled",
               "match":{"kind":"admin","resourceType":"USER",
                        "operations":["UPDATE"],"userEnabled":false},
               "policy":{"action":"discard","maxFailures":2}}]}}
            """);
    listener =
        new DurableEventListener(session, BridgeConfig.from(Map.of()), wakeRelay, () -> filter);
    listener.onEvent(event, false);
    prepare();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    var row = captured.getValue();
    var data = new ObjectMapper().readTree(row.payload()).path("data");
    assertEquals(id, data.path("userId").asText());
    assertEquals("1", data.path("ordering").path("sequence").asText());
    assertEquals(new EventOrdering("realm", id, 1).key(), row.orderingKey());
    assertEquals("disabled", row.publicationPolicy().ruleId());
    assertEquals(2, row.publicationPolicy().policy().maxFailures());
    assertEquals(BooleanNode.FALSE, data.get("userEnabled"));
    verify(users).getUserById(realm, id);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ACTION", "DELETE"})
  void routedNestedActionsAndDeletedUsersDoNotRequireAccountLookup(String operation)
      throws Exception {
    String id = "f:provider:opaque/consents";
    var event = EventEnvelopeTest.admin(OperationType.valueOf(operation));
    event.setResourcePath("users/" + id + (operation.equals("ACTION") ? "/reset-password" : ""));
    var request = AdminRequestFixtures.request(event, id, Map.of());
    var context = request.getContext();
    when(session.getContext()).thenReturn(context);
    listener.onEvent(event, false);
    prepare();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    var data = new ObjectMapper().readTree(captured.getValue().payload()).path("data");
    assertEquals(id, data.path("userId").asText());
    assertFalse(data.has("userEnabled"));
    verify(session, never()).realms();
    verify(session, never()).users();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void missingRealmOrUserDoesNotInventAnEnablementState(boolean realmExists) throws Exception {
    var realms = mock(RealmProvider.class);
    var users = mock(UserProvider.class);
    when(session.realms()).thenReturn(realms);
    if (realmExists) {
      when(realms.getRealm("realm")).thenReturn(mock(RealmModel.class));
      when(session.users()).thenReturn(users);
    }
    listener.onEvent(EventEnvelopeTest.admin(OperationType.CREATE), false);
    prepare();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em).persist(captured.capture());
    var data = new ObjectMapper().readTree(captured.getValue().payload()).get("data");
    assertEquals("target", data.get("userId").textValue());
    assertFalse(data.has("userEnabled"));
    if (!realmExists) {
      verifyNoInteractions(users);
      verify(session, never()).users();
    }
  }

  @Test
  void failedOrNestedAdminOperationsNeverObserveCurrentAccountState() throws Exception {
    var failed = EventEnvelopeTest.admin(OperationType.UPDATE);
    failed.setError("forbidden");
    listener.onEvent(failed, false);
    var nested = EventEnvelopeTest.admin(OperationType.UPDATE);
    nested.setResourcePath("users/target/role-mappings");
    listener.onEvent(nested, false);
    prepare();
    verify(session, never()).realms();
    verify(session, never()).users();
    var captured = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em, times(2)).persist(captured.capture());
    for (OutboxEvent event : captured.getAllValues()) {
      assertFalse(new ObjectMapper().readTree(event.payload()).get("data").has("userEnabled"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void completionResetsNotificationCoalescingForTheNextTransaction(boolean commit) {
    listener.onEvent(EventEnvelopeTest.login());
    var callback = ArgumentCaptor.forClass(KeycloakTransaction.class);
    verify(tx).enlistAfterCompletion(callback.capture());
    callback.getValue().begin();
    if (commit) {
      callback.getValue().commit();
    } else {
      callback.getValue().rollback();
    }
    listener.onEvent(EventEnvelopeTest.login());
    verify(tx, times(2)).enlistAfterCompletion(any());
    verify(wakeRelay, times(commit ? 1 : 0)).run();
    listener.close();
    verify(tx, never()).commit();
    verify(em, never()).close();
  }

  private void prepare() {
    var callback = ArgumentCaptor.forClass(KeycloakTransaction.class);
    verify(tx).enlistPrepare(callback.capture());
    callback.getValue().begin();
    callback.getValue().commit();
  }

  @Test
  void orderedLocksPreserveEachUsersCallbackSnapshots() throws Exception {
    var event = EventEnvelopeTest.login();
    event.setUserId("z");
    event.setSessionId("z-first");
    listener.onEvent(event);
    event.setUserId("a");
    event.setSessionId("a-first");
    listener.onEvent(event);
    event.setUserId("z");
    event.setSessionId("z-second");
    listener.onEvent(event);
    verifyNoInteractions(em);
    prepare();
    var rows = ArgumentCaptor.forClass(OutboxEvent.class);
    verify(em, times(3)).persist(rows.capture());
    var mapper = new ObjectMapper();
    var snapshots = new java.util.ArrayList<String>();
    for (var row : rows.getAllValues()) {
      snapshots.add(mapper.readTree(row.payload()).at("/data/sessionId").asText());
    }
    assertEquals(List.of("a-first", "z-first", "z-second"), snapshots);
    String a = new EventOrdering(event.getRealmId(), "a", 1).key();
    String z = new EventOrdering(event.getRealmId(), "z", 1).key();
    heads.verify(() -> OutboxHeads.refresh(em, a));
    heads.verify(() -> OutboxHeads.refresh(em, z));
  }

  @Test
  void userlessEventsNeverUpdateHeads() {
    var event = EventEnvelopeTest.login();
    event.setUserId(null);
    listener.onEvent(event);
    prepare();
    verify(em).persist(any());
    heads.verifyNoInteractions();
  }

  @Test
  void callbacksAfterPrepareFailClosed() {
    listener.onEvent(EventEnvelopeTest.login());
    prepare();
    assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
    verify(tx).setRollbackOnly();
    verify(em).persist(any());
  }

  @Test
  void prepareEnlistmentFailureMarksRollbackBeforeAnyPersistence() {
    doThrow(new IllegalStateException("cannot enlist")).when(tx).enlistPrepare(any());
    assertThrows(IllegalStateException.class, () -> listener.onEvent(EventEnvelopeTest.login()));
    verify(tx).setRollbackOnly();
    verifyNoInteractions(em, wakeRelay);
  }

  @Test
  void rollingBackPreparationDiscardsItsBuffer() {
    listener.onEvent(EventEnvelopeTest.login());
    var callback = ArgumentCaptor.forClass(KeycloakTransaction.class);
    verify(tx).enlistPrepare(callback.capture());
    callback.getValue().begin();
    callback.getValue().rollback();
    verifyNoInteractions(em, wakeRelay);
  }
}
