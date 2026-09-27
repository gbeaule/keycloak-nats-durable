package io.github.gbeaule.keycloaknats;

import org.keycloak.events.Event;
import org.keycloak.events.admin.AdminEvent;

final class CaptureFixtures {
  static final ResolvedPublicationPolicy RETRY =
      new ResolvedPublicationPolicy(PublicationPolicy.RETRY, EventFilter.digest(new byte[0]), null);

  private CaptureFixtures() {}

  static OutboxEvent user(EventEnvelope encoder, Event event) {
    return user(encoder, event, null);
  }

  static OutboxEvent user(EventEnvelope encoder, Event event, EventOrdering ordering) {
    return encoder.serialize(encoder.describe(event), ordering, System.currentTimeMillis(), RETRY);
  }

  static OutboxEvent admin(EventEnvelope encoder, AdminEvent event, Boolean enabled) {
    return admin(encoder, event, enabled, null);
  }

  static OutboxEvent admin(
      EventEnvelope encoder, AdminEvent event, Boolean enabled, EventOrdering ordering) {
    return encoder.serialize(
        encoder.describe(event, enabled), ordering, System.currentTimeMillis(), RETRY);
  }

  static OutboxEvent row(String id, String subject, String payload, long capturedAt) {
    return new OutboxEvent(
        id, subject, payload, capturedAt, "realm", "io.keycloak.user.login", null, RETRY);
  }
}
