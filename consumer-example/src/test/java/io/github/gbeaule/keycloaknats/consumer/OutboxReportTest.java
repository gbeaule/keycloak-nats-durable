package io.github.gbeaule.keycloaknats.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OutboxReportTest {
  @ParameterizedTest
  @ValueSource(strings = {">", "events.tenant[1].*"})
  void aggregateCollectionUsesBoundedReadOnlySnapshotsAndPreservesDatabaseCounts(String subject)
      throws Exception {
    var db = mock(Connection.class);
    var source = source(db);
    var settings = mock(PreparedStatement.class);
    var backlog = query(db, "WITH selected");
    var audits = query(db, "WHERE reason");
    var storage = query(db, "pg_total_relation_size");
    when(db.prepareStatement(contains("set_config"))).thenReturn(settings);
    var pending = backlog.executeQuery();
    when(pending.getLong(1)).thenReturn(12L);
    when(pending.getDouble(2)).thenReturn(6.25);
    when(pending.getLong(5)).thenReturn(8L);
    when(pending.getLong(6)).thenReturn(3L);
    when(pending.getLong(7)).thenReturn(2L);
    when(pending.getDouble(9)).thenReturn(1234.0);
    when(audits.executeQuery().getLong(1)).thenReturn(5L);
    when(audits.executeQuery().getLong(4)).thenReturn(2L);
    when(audits.executeQuery().getLong(5)).thenReturn(1L);
    when(storage.executeQuery().getLong(1)).thenReturn(4096L);

    var report = OutboxReport.collect(source, "tenant-data", subject, "events.tenant", 5, 60);
    assertEquals(12L, report.get("pending"));
    assertEquals(6.25, report.get("oldest_age_seconds"));
    assertEquals(8L, report.get("blocked_events"));
    assertEquals(3L, report.get("blocked_users"));
    assertEquals(2L, report.get("retrying_heads"));
    assertEquals(5L, report.get("audit_retained"));
    assertEquals(2L, report.get("audit_retained_unknown"));
    assertEquals(1L, report.get("audit_eligible"));
    assertEquals(1234.0, report.get("collected_at_timestamp_seconds"));
    assertTrue(OutboxReport.prometheus(report).contains("knd_outbox_blocked_users 3\n"));
    verify(db).setReadOnly(true);
    verify(db).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    verify(db).setAutoCommit(false);
    verify(settings).setString(1, "5000");
    verify(settings).setQueryTimeout(5);
    String pattern =
        subject.equals(">") ? "^[^.]+(\\.[^.]+)*$" : "^events\\.tenant\\[1\\]\\.[^.]+$";
    verify(backlog).setString(1, subject.equals(">") ? "" : pattern);
    verify(backlog).setString(2, pattern);
    verify(backlog).setString(3, "events.tenant");
    verify(backlog).setString(4, "events.tenant");
    verify(audits).setLong(1, 60000L);
    verify(audits).setLong(2, 60000L);
    verify(audits).setString(3, subject.equals(">") ? "" : pattern);
    verify(audits).setString(4, pattern);
    verify(audits).setString(5, "events.tenant");
    verify(audits).setString(6, "events.tenant");
    verify(storage).setString(1, "\"tenant-data\".\"kc_nats_outbox\"");
    verify(storage).setString(2, "tenant-data");
    verify(backlog).setQueryTimeout(5);
    verify(audits).setQueryTimeout(5);
    verify(storage).setQueryTimeout(5);
    verify(db).commit();
    verify(db).close();
  }

  @Test
  void inspectionPreservesMetadataTypesAndBindsBoundedCursors() throws Exception {
    var db = mock(Connection.class);
    var source = source(db);
    when(db.prepareStatement(contains("set_config"))).thenReturn(mock(PreparedStatement.class));
    var query = query(db, "ORDER BY discarded_at");
    var rows = query.executeQuery();
    when(rows.next()).thenReturn(true, false);
    var metadata = mock(ResultSetMetaData.class);
    when(rows.getMetaData()).thenReturn(metadata);
    when(metadata.getColumnCount()).thenReturn(3);
    when(metadata.getColumnLabel(1)).thenReturn("id");
    when(metadata.getColumnLabel(2)).thenReturn("user_sequence");
    when(metadata.getColumnLabel(3)).thenReturn("publication_may_have_occurred");
    when(rows.getObject(1)).thenReturn("event");
    when(rows.getObject(2)).thenReturn(15L);
    when(rows.getObject(3)).thenReturn(true);
    var result = OutboxReport.inspect(source, "tenant\"schema", 2, 42, "previous", 10);
    assertEquals(
        Map.of("id", "event", "user_sequence", 15L, "publication_may_have_occurred", true),
        result.getFirst());
    verify(db).prepareStatement(contains("\"tenant\"\"schema\".\"kc_nats_discard_audit\""));
    verify(query).setLong(1, 42);
    verify(query).setString(2, "previous");
    verify(query).setInt(3, 10);
    verify(query).setQueryTimeout(2);
    verify(db).setReadOnly(true);
    verify(db).commit();
    verify(db).close();
  }

  @Test
  void failedSetupOrCollectionClosesTheConnectionWithoutReportingSuccess() throws Exception {
    var db = mock(Connection.class);
    var source = source(db);
    when(db.prepareStatement(anyString())).thenThrow(new SQLException("unavailable"));
    assertThrows(SQLException.class, () -> OutboxReport.collect(source, "public", ">", "", 5));
    verify(db).close();
    verify(db, never()).commit();
  }

  private static DataSource source(Connection db) throws Exception {
    var source = mock(DataSource.class);
    when(source.getConnection()).thenReturn(db);
    return source;
  }

  private static PreparedStatement query(Connection db, String text) throws Exception {
    var query = mock(PreparedStatement.class);
    var rows = mock(ResultSet.class);
    when(db.prepareStatement(contains(text))).thenReturn(query);
    when(query.executeQuery()).thenReturn(rows);
    when(rows.next()).thenReturn(true);
    return query;
  }

  @Test
  void reportFilterTreatsSqlAndRegexMetacharactersAsLiteralSubjectContent() {
    var regex = Pattern.compile(OutboxReport.sqlPattern("events.tenant[1].*"));
    assertTrue(regex.matcher("events.tenant[1].login").matches());
    assertFalse(regex.matcher("events.tenant1.login").matches());
    assertFalse(regex.matcher("events.tenant[1].admin.login").matches());
    var terminal = Pattern.compile(OutboxReport.sqlPattern("events.>"));
    assertTrue(terminal.matcher("events.a.login").matches());
    assertFalse(terminal.matcher("events").matches());
  }
}
