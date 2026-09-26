package io.github.gbeaule.keycloaknats;

import java.util.List;
import org.keycloak.Config;
import org.keycloak.connections.jpa.entityprovider.JpaEntityProvider;
import org.keycloak.connections.jpa.entityprovider.JpaEntityProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

/** Registers the outbox entity and Liquibase changelog in Keycloak's persistence unit. */
public final class OutboxEntityProviderFactory implements JpaEntityProviderFactory {
  @Override
  public JpaEntityProvider create(KeycloakSession session) {
    return new JpaEntityProvider() {
      @Override
      public List<Class<?>> getEntities() {
        return List.of(OutboxEvent.class);
      }

      @Override
      public String getChangelogLocation() {
        return "META-INF/nats-outbox-changelog.xml";
      }

      @Override
      public String getFactoryId() {
        return getId();
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public String getId() {
    return "nats-durable-outbox";
  }

  @Override
  public void init(Config.Scope scope) {}

  @Override
  public void postInit(KeycloakSessionFactory factory) {}

  @Override
  public void close() {}
}
