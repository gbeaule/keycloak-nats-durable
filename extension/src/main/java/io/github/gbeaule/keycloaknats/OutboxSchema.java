package io.github.gbeaule.keycloaknats;

import java.util.ArrayList;
import liquibase.change.custom.CustomSqlChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Table;

/**
 * PostgreSQL constraints, partial indexes and maintenance settings for the initial schema. Keycloak
 * installs extension schemas through Liquibase, not Hibernate schema generation.
 */
public final class OutboxSchema implements CustomSqlChange {
  private static final String VACUUM_SETTINGS =
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

  private static final String EVENT_CHECKS =
      """
      ADD CONSTRAINT CK_%s_ORDERING CHECK (
        (ORDERING_KEY IS NULL AND USER_SEQUENCE IS NULL) OR
        (ORDERING_KEY IS NOT NULL AND USER_SEQUENCE IS NOT NULL AND USER_SEQUENCE > 0)),
      ADD CONSTRAINT CK_%s_POLICY CHECK (
        (MAX_AGE_SECONDS IS NULL OR MAX_AGE_SECONDS BETWEEN 1 AND 31536000) AND
        (MAX_FAILURES IS NULL OR MAX_FAILURES BETWEEN 1 AND 1000000) AND
        ((MAX_AGE_SECONDS IS NULL AND EXPIRES_AT IS NULL) OR
         (MAX_AGE_SECONDS IS NOT NULL AND EXPIRES_AT IS NOT NULL AND
          EXPIRES_AT = CREATED_AT + MAX_AGE_SECONDS::bigint * 1000))),
      ADD CONSTRAINT CK_%s_ATTEMPTS CHECK (ATTEMPTS >= 0)
      """;

  @Override
  public SqlStatement[] generateStatements(Database database) {
    var statements = new ArrayList<SqlStatement>();
    for (String name : new String[] {"KC_NATS_OUTBOX", "KC_NATS_DISCARD_AUDIT"}) {
      String checks = EVENT_CHECKS.formatted(name, name, name);
      if (name.equals("KC_NATS_DISCARD_AUDIT")) {
        checks +=
            ", ADD CONSTRAINT CK_KC_NATS_DISCARD_REASON"
                + " CHECK (REASON IN ('EXPIRED', 'MAX_FAILURES'))";
      }
      statements.add(new RawSqlStatement("ALTER TABLE " + table(database, name) + checks));
    }
    statements.add(
        new RawSqlStatement(
            "ALTER TABLE "
                + table(database, "KC_NATS_CAPTURE_COUNTER")
                + " ADD CONSTRAINT CK_KC_NATS_COUNTER_SEQUENCE CHECK (LAST_SEQUENCE > 0),"
                + " ADD CONSTRAINT CK_KC_NATS_COUNTER_HEAD CHECK ("
                + "(HEAD_EVENT_ID IS NULL AND HEAD_NEXT_ATTEMPT_AT IS NULL"
                + " AND HEAD_CREATED_AT IS NULL)"
                + " OR (HEAD_EVENT_ID IS NOT NULL AND HEAD_NEXT_ATTEMPT_AT IS NOT NULL"
                + " AND HEAD_CREATED_AT IS NOT NULL))"));
    statements.add(
        new RawSqlStatement(
            "CREATE INDEX IDX_KC_NATS_HEAD_DUE ON "
                + table(database, "KC_NATS_CAPTURE_COUNTER")
                + " (HEAD_NEXT_ATTEMPT_AT, HEAD_CREATED_AT, HEAD_EVENT_ID)"
                + " WHERE HEAD_EVENT_ID IS NOT NULL"));
    statements.add(
        new RawSqlStatement(
            "CREATE INDEX IDX_KC_NATS_INDEPENDENT_DUE ON "
                + table(database, "KC_NATS_OUTBOX")
                + " (NEXT_ATTEMPT_AT, CREATED_AT, ID) WHERE ORDERING_KEY IS NULL"));
    statements.add(
        new RawSqlStatement("ALTER TABLE " + table(database, "KC_NATS_OUTBOX") + VACUUM_SETTINGS));
    return statements.toArray(SqlStatement[]::new);
  }

  private static String table(Database database, String name) {
    return database.escapeTableName(
            database.getDefaultCatalogName(),
            database.getDefaultSchemaName(),
            database.correctObjectName(name, Table.class))
        + " ";
  }

  @Override
  public String getConfirmationMessage() {
    return "Configured outbox constraints, indexes and autovacuum";
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
