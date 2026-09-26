package io.github.keycloaknats;

import liquibase.change.custom.CustomSqlChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Table;

/** Generates PostgreSQL maintenance settings using Liquibase's schema and identifier quoting. */
public final class OutboxVacuumMigration implements CustomSqlChange {
  private static final String SETTINGS =
      """
      SET (
        autovacuum_vacuum_scale_factor = 0.02,
        autovacuum_vacuum_threshold = 50,
        autovacuum_analyze_scale_factor = 0.05,
        autovacuum_analyze_threshold = 50,
        toast.autovacuum_vacuum_scale_factor = 0.02,
        toast.autovacuum_vacuum_threshold = 50
      )
      """;

  @Override
  public SqlStatement[] generateStatements(Database database) {
    String table =
        database.escapeTableName(
            database.getDefaultCatalogName(),
            database.getDefaultSchemaName(),
            database.correctObjectName("KC_NATS_OUTBOX", Table.class));
    return new SqlStatement[] {new RawSqlStatement("ALTER TABLE " + table + " " + SETTINGS)};
  }

  @Override
  public String getConfirmationMessage() {
    return "Configured outbox and TOAST autovacuum";
  }

  @Override
  public void setUp() {}

  @Override
  public void setFileOpener(ResourceAccessor resourceAccessor) {}

  @Override
  public ValidationErrors validate(Database database) {
    return new ValidationErrors();
  }
}
