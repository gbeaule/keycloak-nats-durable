package io.github.gbeaule.keycloaknats;

import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;

/** Resolves Keycloak's internal admin client ID to the public ID in the actor's realm. */
final class ActingClient {
  private ActingClient() {}

  static String resolve(AdminEvent event, KeycloakSession session) {
    var auth = event.getAuthDetails();
    if (auth == null || auth.getClientId() == null || auth.getRealmId() == null) {
      return null;
    }
    var realm = session.realms().getRealm(auth.getRealmId());
    if (realm == null) {
      return null;
    }
    var client = session.clients().getClientById(realm, auth.getClientId());
    // Never substitute the UUID: unknown public IDs must not change the meaning of clientId.
    return client == null ? null : client.getClientId();
  }
}
