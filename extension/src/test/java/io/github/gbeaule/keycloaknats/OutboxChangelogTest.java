package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import liquibase.ChecksumVersion;
import liquibase.change.CheckSum;
import liquibase.change.custom.CustomChangeWrapper;
import liquibase.changelog.ChangeLogParameters;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;

/** Protects existing outbox databases when migration classes move to a new package. */
class OutboxChangelogTest {
  @Test
  void packageRenameAcceptsPreviouslyAppliedMigrations() throws Exception {
    try (var resources = new ClassLoaderResourceAccessor()) {
      var changelog =
          new XMLChangeLogSAXParser()
              .parse("META-INF/nats-outbox-changelog.xml", new ChangeLogParameters(), resources);
      var creation =
          changelog.getChangeSet(
              "META-INF/nats-outbox-changelog.xml", "keycloak-nats-durable", "nats-outbox-1");
      assertEquals(
          "9:154d3cc9eb4f528687155cc6ef8a5c69",
          creation.generateCheckSum(ChecksumVersion.V9).toString());

      var vacuum =
          changelog.getChangeSet(
              "META-INF/nats-outbox-changelog.xml",
              "keycloak-nats-durable",
              "nats-outbox-2-vacuum");
      assertTrue(vacuum.isCheckSumValid(CheckSum.parse("9:0865fbeca5c396e9cd44e1b50720d5ca")));
      assertFalse(vacuum.isCheckSumValid(CheckSum.parse("9:00000000000000000000000000000000")));
      var change = assertInstanceOf(CustomChangeWrapper.class, vacuum.getChanges().getFirst());
      assertInstanceOf(OutboxVacuumMigration.class, change.getCustomChange());
    }
  }
}
