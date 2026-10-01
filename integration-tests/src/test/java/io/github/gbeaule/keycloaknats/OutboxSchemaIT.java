package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.core.PostgresDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;

/** Installs the complete initial schema and preserves data when Liquibase runs again on restart. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OutboxSchemaIT {
  @Test
  void initialSetupIsCompleteAndRunsOnce() throws Exception {
    try (var postgres = IntegrationSupport.postgresContainer()) {
      postgres.start();
      try (var connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          var resources = new ClassLoaderResourceAccessor()) {
        execute(connection, "CREATE SCHEMA initial_setup");
        var database = new PostgresDatabase();
        database.setConnection(new JdbcConnection(connection));
        database.setDefaultSchemaName("initial_setup");
        try (var setup = new Liquibase("META-INF/nats-outbox-changelog.xml", resources, database)) {
          setup.update(new Contexts());
          execute(
              connection,
              """
              INSERT INTO initial_setup.kc_nats_capture_counter
                (ordering_key,realm_id,user_id,last_sequence,
                 head_event_id,head_next_attempt_at,head_created_at)
              VALUES ('key','realm','user',20,'first',1000,5),
                     ('empty','realm','empty',9,NULL,NULL,NULL)
              """);
          execute(
              connection,
              """
              INSERT INTO initial_setup.kc_nats_outbox
                (id,realm_id,event_type,subject,payload_sha256,filter_sha256,payload,
                 created_at,next_attempt_at,ordering_key,user_sequence)
              SELECT id,'realm','io.keycloak.user.login','keycloak.events.realm.user.login',
                repeat('a',64),repeat('b',64),json_build_object('id',id)::text,
                created_at,next_attempt_at,ordering_key,user_sequence
              FROM (VALUES ('first',5,1000,'key',10), ('second',6,0,'key',20),
                           ('independent',7,0,NULL,NULL))
                AS event(id,created_at,next_attempt_at,ordering_key,user_sequence)
              """);
          String snapshot =
              "SELECT row_to_json(event)::text FROM initial_setup.kc_nats_outbox event"
                  + " ORDER BY id";
          var originals = query(connection, snapshot);
          setup.update(new Contexts());
          assertEquals(originals, query(connection, snapshot));
          assertEquals(
              List.of("[\"empty\", 9, null, null, null]", "[\"key\", 20, \"first\", 1000, 5]"),
              query(
                  connection,
                  "SELECT json_build_array(ordering_key,last_sequence,head_event_id,"
                      + " head_next_attempt_at,head_created_at)::text"
                      + " FROM initial_setup.kc_nats_capture_counter ORDER BY ordering_key"));
          assertEquals(
              List.of("1"),
              query(connection, "SELECT count(*) FROM initial_setup.databasechangelog"));
          assertEquals(
              List.of("idx_kc_nats_head_due", "idx_kc_nats_independent_due"),
              query(
                  connection,
                  "SELECT indexname FROM pg_indexes WHERE schemaname='initial_setup'"
                      + " AND indexdef LIKE '% WHERE %' ORDER BY indexname"));
          connection.setAutoCommit(true);
          var incompleteHead =
              assertThrows(
                  SQLException.class,
                  () ->
                      execute(
                          connection,
                          "UPDATE initial_setup.kc_nats_capture_counter"
                              + " SET head_created_at=NULL WHERE ordering_key='key'"));
          assertEquals("23514", incompleteHead.getSQLState());
          assertEquals(originals, query(connection, snapshot));
        }
      }
    }
  }

  private static void execute(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static List<String> query(Connection connection, String sql) throws Exception {
    var values = new ArrayList<String>();
    try (var statement = connection.createStatement();
        var rows = statement.executeQuery(sql)) {
      while (rows.next()) {
        values.add(rows.getString(1));
      }
    }
    return values;
  }
}
