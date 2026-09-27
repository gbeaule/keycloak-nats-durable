package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import liquibase.change.core.AddUniqueConstraintChange;
import liquibase.change.core.CreateIndexChange;
import liquibase.change.core.CreateTableChange;
import liquibase.changelog.ChangeLogParameters;
import liquibase.database.core.PostgresDatabase;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.api.Test;

class OutboxChangelogTest {
  @Test
  void freshSchemaSeparatesCapturePublicationAndMetadataOnlyDiagnostics() throws Exception {
    try (var resources = new ClassLoaderResourceAccessor()) {
      var changelog =
          new XMLChangeLogSAXParser()
              .parse("META-INF/nats-outbox-changelog.xml", new ChangeLogParameters(), resources);
      var changes = changelog.getChangeSets().getFirst().getChanges();
      var tables =
          changes.stream()
              .filter(CreateTableChange.class::isInstance)
              .map(CreateTableChange.class::cast)
              .toList();
      assertEquals(
          List.of("KC_NATS_OUTBOX", "KC_NATS_DISCARD_AUDIT", "KC_NATS_CAPTURE_COUNTER"),
          tables.stream().map(CreateTableChange::getTableName).toList());
      assertTrue(tables.get(0).getColumns().stream().anyMatch(c -> c.getName().equals("PAYLOAD")));
      assertFalse(tables.get(1).getColumns().stream().anyMatch(c -> c.getName().equals("PAYLOAD")));
      assertEquals(
          List.of("REALM_ID,USER_ID", "ORDERING_KEY,USER_SEQUENCE"),
          changes.stream()
              .filter(AddUniqueConstraintChange.class::isInstance)
              .map(AddUniqueConstraintChange.class::cast)
              .map(AddUniqueConstraintChange::getColumnNames)
              .toList());
      assertEquals(3, changes.stream().filter(CreateIndexChange.class::isInstance).count());
    }
  }

  @Test
  void checksUseLiquibaseSchemaQuoting() throws Exception {
    var database = new PostgresDatabase();
    database.setDefaultSchemaName("bridge-data");
    var migration = new CaptureConstraintsMigration();
    migration.setUp();
    migration.setFileOpener(null);
    assertFalse(migration.validate(database).hasErrors());
    assertEquals("Installed capture and discard constraints", migration.getConfirmationMessage());
    var statements = migration.generateStatements(database);
    assertEquals(3, statements.length);
    for (var statement : statements) {
      assertTrue(
          ((RawSqlStatement) statement)
              .getSql()
              .startsWith("ALTER TABLE \"bridge-data\".kc_nats_"));
    }
  }
}
