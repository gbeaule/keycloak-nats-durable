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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OutboxChangelogTest {
  @Test
  void freshSchemaSeparatesCapturePublicationAndMetadataOnlyDiagnostics() throws Exception {
    try (var resources = new ClassLoaderResourceAccessor()) {
      var changelog =
          new XMLChangeLogSAXParser()
              .parse("META-INF/nats-outbox-changelog.xml", new ChangeLogParameters(), resources);
      assertEquals(1, changelog.getChangeSets().size());
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
      assertTrue(
          tables.get(2).getColumns().stream()
              .map(c -> c.getName())
              .toList()
              .containsAll(List.of("HEAD_EVENT_ID", "HEAD_NEXT_ATTEMPT_AT", "HEAD_CREATED_AT")));
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

  @ParameterizedTest
  @ValueSource(strings = {"public", "bridge-data", "schema\"quote"})
  void postgresSetupUsesLiquibaseSchemaQuoting(String schema) throws Exception {
    var database = new PostgresDatabase();
    database.setDefaultSchemaName(schema);
    var setup = new OutboxSchema();
    setup.setUp();
    setup.setFileOpener(null);
    assertFalse(setup.validate(database).hasErrors());
    assertEquals(
        "Configured outbox constraints, indexes and autovacuum", setup.getConfirmationMessage());
    var statements = setup.generateStatements(database);
    assertEquals(6, statements.length);
    String qualifier =
        schema.equals("public") ? "public" : "\"" + schema.replace("\"", "\"\"") + "\"";
    for (int i : new int[] {0, 1, 2, 5}) {
      assertTrue(
          ((RawSqlStatement) statements[i])
              .getSql()
              .startsWith("ALTER TABLE " + qualifier + ".kc_nats_"));
    }
    assertTrue(
        ((RawSqlStatement) statements[3])
            .getSql()
            .contains("ON " + qualifier + ".kc_nats_capture_counter"));
    assertTrue(
        ((RawSqlStatement) statements[4]).getSql().contains("ON " + qualifier + ".kc_nats_outbox"));
  }
}
