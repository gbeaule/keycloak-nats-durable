package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import liquibase.database.core.PostgresDatabase;
import liquibase.statement.core.RawSqlStatement;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class OutboxVacuumMigrationTest {
  @ParameterizedTest
  @CsvSource({
    "public,public.kc_nats_outbox",
    "bridge-data,\"bridge-data\".kc_nats_outbox",
    "schema\"quote,\"schema\"\"quote\".kc_nats_outbox"
  })
  void maintenanceUsesTheConfiguredSchemaAndQuotesIdentifiers(String schema, String table)
      throws Exception {
    var database = new PostgresDatabase();
    database.setDefaultSchemaName(schema);
    database.setDefaultCatalogName("keycloak");
    var migration = new OutboxVacuumMigration();
    migration.setUp();
    migration.setFileOpener(null);
    assertFalse(migration.validate(database).hasErrors());
    var statements = migration.generateStatements(database);
    assertEquals(1, statements.length);
    String sql = assertInstanceOf(RawSqlStatement.class, statements[0]).getSql();
    assertEquals(
        "ALTER TABLE "
            + table
            + " SET ( autovacuum_vacuum_scale_factor = 0.02, "
            + "autovacuum_vacuum_threshold = 50, autovacuum_analyze_scale_factor = 0.05, "
            + "autovacuum_analyze_threshold = 50, toast.autovacuum_vacuum_scale_factor = 0.02, "
            + "toast.autovacuum_vacuum_threshold = 50 )",
        sql.replaceAll("\\s+", " ").trim());
    assertEquals("Configured outbox and TOAST autovacuum", migration.getConfirmationMessage());
  }
}
