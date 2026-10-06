package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.keycloak.events.admin.AuthDetails;
import org.keycloak.events.admin.OperationType;
import org.keycloak.models.ClientModel;
import org.keycloak.models.ClientProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;

class ActingClientTest {
  @ParameterizedTest
  @CsvSource({",management", "internal,", ","})
  void missingSourceIdentifiersNeedNoLookup(String id, String realmId) {
    var event = EventEnvelopeTest.admin(OperationType.UPDATE);
    var session = mock(KeycloakSession.class);
    assertNull(ActingClient.resolve(event, session));
    var auth = new AuthDetails();
    auth.setClientId(id);
    auth.setRealmId(realmId);
    event.setAuthDetails(auth);
    assertNull(ActingClient.resolve(event, session));
    verifyNoInteractions(session);
  }

  @Test
  void unknownRealmsAndDeletedClientsNeverFallBackToAnInternalId() {
    var event = EventEnvelopeTest.admin(OperationType.DELETE);
    var auth = new AuthDetails();
    auth.setClientId("internal");
    auth.setRealmId("management");
    event.setAuthDetails(auth);
    var session = mock(KeycloakSession.class);
    var realms = mock(RealmProvider.class);
    var clients = mock(ClientProvider.class);
    when(session.realms()).thenReturn(realms);
    when(session.clients()).thenReturn(clients);
    assertNull(ActingClient.resolve(event, session));
    verifyNoInteractions(clients);
    var realm = mock(RealmModel.class);
    when(realms.getRealm("management")).thenReturn(realm);
    assertNull(ActingClient.resolve(event, session));
    verify(clients).getClientById(realm, "internal");
  }

  @Test
  void lookupUsesTheActingRealmAndPreservesTheExactPublicId() {
    var event = EventEnvelopeTest.admin(OperationType.UPDATE);
    var auth = new AuthDetails();
    auth.setClientId("internal");
    auth.setRealmId("management");
    event.setAuthDetails(auth);
    var session = mock(KeycloakSession.class);
    var realms = mock(RealmProvider.class);
    var clients = mock(ClientProvider.class);
    var realm = mock(RealmModel.class);
    var client = mock(ClientModel.class);
    when(session.realms()).thenReturn(realms);
    when(session.clients()).thenReturn(clients);
    when(realms.getRealm("management")).thenReturn(realm);
    when(clients.getClientById(realm, "internal")).thenReturn(client);
    when(client.getClientId()).thenReturn("Admin CLI.+");
    assertEquals("Admin CLI.+", ActingClient.resolve(event, session));
    assertEquals("internal", auth.getClientId(), "Never mutate Keycloak's source event");
    verify(realms).getRealm("management");
    verify(clients).getClientById(realm, "internal");
  }
}
