package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.keycloak.Config;
import org.keycloak.connections.jpa.entityprovider.JpaEntityProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

class OutboxEntityProviderFactoryTest {
  @Test
  void registeredFactoryExposesCaptureEntitiesAndPackagedMigrationWithoutOpeningDatabase() {
    var factory =
        ServiceLoader.load(JpaEntityProviderFactory.class).stream()
            .filter(provider -> provider.type().equals(OutboxEntityProviderFactory.class))
            .findFirst()
            .orElseThrow()
            .get();
    var scope = mock(Config.Scope.class);
    var sessions = mock(KeycloakSessionFactory.class);
    final var session = mock(KeycloakSession.class);
    factory.init(scope);
    factory.postInit(sessions);
    assertEquals("nats-durable-outbox", factory.getId());
    var provider = factory.create(session);
    try {
      assertEquals(
          List.of(OutboxEvent.class, CaptureCounter.class, DiscardAudit.class),
          provider.getEntities());
      assertEquals(factory.getId(), provider.getFactoryId());
      assertEquals("META-INF/nats-outbox-changelog.xml", provider.getChangelogLocation());
      assertNotNull(getClass().getClassLoader().getResource(provider.getChangelogLocation()));
    } finally {
      provider.close();
    }
    factory.close();
    verifyNoInteractions(scope, sessions, session);
  }
}
