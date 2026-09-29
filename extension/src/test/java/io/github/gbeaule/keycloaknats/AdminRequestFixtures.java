package io.github.gbeaule.keycloaknats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.MultivaluedHashMap;
import java.util.Map;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakUriInfo;
import org.keycloak.models.RealmModel;

final class AdminRequestFixtures {
  private AdminRequestFixtures() {}

  static KeycloakSession request(AdminEvent event, String userId, Map<String, String> nested) {
    var parameters = new MultivaluedHashMap<String, String>();
    if (userId != null) {
      parameters.add("user-id", userId);
    }
    nested.forEach(parameters::add);
    var session = mock(KeycloakSession.class);
    var context = mock(KeycloakContext.class);
    var realm = mock(RealmModel.class);
    var uri = mock(KeycloakUriInfo.class);
    when(session.getContext()).thenReturn(context);
    when(context.getRealm()).thenReturn(realm);
    when(context.getUri()).thenReturn(uri);
    when(realm.getId()).thenReturn(event.getRealmId());
    when(realm.getName()).thenReturn("realm-name");
    when(uri.getPathParameters()).thenReturn(parameters);
    when(uri.getPath()).thenReturn("/admin/realms/realm-name/" + event.getResourcePath());
    return session;
  }
}
