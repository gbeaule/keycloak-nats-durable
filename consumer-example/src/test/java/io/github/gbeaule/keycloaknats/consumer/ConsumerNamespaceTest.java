package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.nats.client.api.ConsumerConfiguration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConsumerNamespaceTest {
  private static final String PREFIX = "keycloak.events";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "keycloak.events.>",
        "keycloak.events.*.user.login",
        "keycloak.events.realm.admin.user.update",
        "keycloak.events.*.admin.*.*"
      })
  void acceptsFullNamespaceAndIntentionalSingleFilterSubsets(String subject) {
    var consumer = ConsumerConfiguration.builder().filterSubject(subject).build();
    assertDoesNotThrow(
        () -> ConsumerMain.validateSubjectNamespace(consumer.getFilterSubjects(), PREFIX));
  }

  @Test
  void acceptsMultipleFiltersOnlyWhenEveryFilterBelongsToTheNamespace() {
    var allowed =
        ConsumerConfiguration.builder()
            .filterSubjects("keycloak.events.*.user.login", "keycloak.events.*.admin.user.update")
            .build();
    assertDoesNotThrow(
        () -> ConsumerMain.validateSubjectNamespace(allowed.getFilterSubjects(), PREFIX));
    var mixed =
        ConsumerConfiguration.builder()
            .filterSubjects("keycloak.events.*.user.login", "other.events.>")
            .build();
    assertThrows(
        IllegalStateException.class,
        () -> ConsumerMain.validateSubjectNamespace(mixed.getFilterSubjects(), PREFIX));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "other.events.>",
        "keycloak.events-other.>",
        "keycloak.events",
        "keycloak.*.>",
        "*.events.>",
        "keycloak.>",
        ">"
      })
  void rejectsWrongNamespacesAndPatternsThatCanEscapeTheNamespace(String subject) {
    assertThrows(
        IllegalStateException.class,
        () -> ConsumerMain.validateSubjectNamespace(List.of(subject), PREFIX));
  }

  @ParameterizedTest
  @ValueSource(strings = {"keycloak.events.", "keycloak.events.>.login", "keycloak.events.a b"})
  void rejectsMalformedPatternsEvenWhenTheyBeginWithTheConfiguredPrefix(String subject) {
    assertThrows(
        IllegalArgumentException.class,
        () -> ConsumerMain.validateSubjectNamespace(List.of(subject), PREFIX));
  }

  @Test
  void anUnfilteredDurableRequiresExplicitBoundedStreamSubjects() {
    assertDoesNotThrow(
        () -> ConsumerMain.validateSubjectNamespace(List.of("keycloak.events.>"), PREFIX));
    assertThrows(
        IllegalStateException.class,
        () ->
            ConsumerMain.validateSubjectNamespace(
                List.of("keycloak.events.>", "other.events.>"), PREFIX));
    assertThrows(
        IllegalStateException.class,
        () -> ConsumerMain.validateSubjectNamespace(List.of(), PREFIX));
    assertThrows(
        IllegalStateException.class, () -> ConsumerMain.validateSubjectNamespace(null, PREFIX));
  }
}
