package io.github.gbeaule.keycloaknats.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gbeaule.keycloaknats.routing.SubjectPattern;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/** One-shot, read-only backlog collector for an owner-managed scheduler or textfile exporter. */
public final class OutboxReport {
  private OutboxReport() {}

  /**
   * Exit codes: zero healthy, one owner threshold exceeded, two collection/configuration failure.
   */
  public static void main(String[] args) {
    boolean prometheus = args.length == 1 && "prometheus".equals(args[0]);
    try {
      var settings = ConsumerConfig.system().report();
      if (args.length > 0 && "audit".equals(args[0])) {
        if (args.length != 1 && args.length != 4) {
          throw new IllegalArgumentException("Expected audit [afterDiscardedAt afterId limit]");
        }
        var rows =
            inspect(
                settings.database(),
                settings.schema(),
                settings.timeoutSeconds(),
                args.length == 1 ? Long.MIN_VALUE : Long.parseLong(args[1]),
                args.length == 1 ? "" : args[2],
                args.length == 1 ? 100 : Integer.parseInt(args[3]));
        System.out.println(new ObjectMapper().writeValueAsString(rows));
        return;
      }
      if (args.length > 1 || (args.length == 1 && !prometheus && !"json".equals(args[0]))) {
        throw new IllegalArgumentException("Expected json or prometheus");
      }
      Map<String, Number> report =
          collect(
              settings.database(),
              settings.schema(),
              settings.subject(),
              settings.realmPrefix(),
              settings.timeoutSeconds(),
              settings.auditRetentionSeconds());
      long maxRows = settings.maxRows();
      long maxAge = settings.maxAgeSeconds();
      boolean exceeded =
          (maxRows > 0 && report.get("pending").longValue() >= maxRows)
              || (maxAge > 0 && report.get("oldest_age_seconds").doubleValue() >= maxAge);
      report.put("threshold_exceeded", exceeded ? 1 : 0);
      if (prometheus) {
        System.out.print(prometheus(report));
      } else {
        System.out.println(new ObjectMapper().writeValueAsString(report));
      }
      if (exceeded) {
        System.exit(1);
      }
    } catch (Exception failure) {
      System.out.println(prometheus ? "knd_outbox_scrape_success 0" : "{\"scrape_success\":0}");
      System.err.println(
          "Outbox collection failed; category=" + failure.getClass().getSimpleName());
      System.exit(2);
    }
  }

  /** Counts selected rows without reading event payloads, using a bounded read-only transaction. */
  public static Map<String, Number> collect(
      DataSource database, String schema, String subject, String realmPrefix, int timeoutSeconds)
      throws Exception {
    return collect(
        database,
        schema,
        subject,
        realmPrefix,
        timeoutSeconds,
        ConsumerConfig.DEFAULT_AUDIT_RETENTION_SECONDS);
  }

  /** Use the provider's retention setting to measure eligibility against the same policy. */
  public static Map<String, Number> collect(
      DataSource database,
      String schema,
      String subject,
      String realmPrefix,
      int timeoutSeconds,
      int auditRetentionSeconds)
      throws Exception {
    if (auditRetentionSeconds < ConsumerConfig.MIN_AUDIT_RETENTION_SECONDS
        || auditRetentionSeconds > ConsumerConfig.MAX_AUDIT_RETENTION_SECONDS) {
      throw new IllegalArgumentException("Invalid audit retention");
    }
    String table = table(schema, "kc_nats_outbox");
    String pattern = sqlPattern(subject);
    var report = new LinkedHashMap<String, Number>();
    try (var db = open(database, timeoutSeconds)) {
      String sql =
          """
          WITH selected AS (
            SELECT event.created_at, event.attempts, event.next_attempt_at, event.ordering_key,
              event.publication_may_have_occurred,
              event.ordering_key IS NOT NULL AND EXISTS (
                SELECT 1 FROM %s predecessor
                WHERE predecessor.ordering_key = event.ordering_key
                  AND predecessor.user_sequence < event.user_sequence
              ) AS blocked
            FROM %s event WHERE (? = '' OR subject ~ ?) AND (? = '' OR starts_with(subject,?))
          )
          SELECT count(*),
            greatest(0,coalesce(extract(epoch FROM transaction_timestamp())-min(created_at)/1000.0,0)),
            coalesce(max(attempts),0),
            count(*) FILTER (WHERE NOT blocked AND next_attempt_at <= extract(epoch FROM transaction_timestamp())*1000),
            count(*) FILTER (WHERE blocked),
            count(DISTINCT ordering_key) FILTER (WHERE blocked),
            count(*) FILTER (WHERE NOT blocked AND attempts > 0),
            count(*) FILTER (WHERE publication_may_have_occurred),
            extract(epoch FROM transaction_timestamp())
          FROM selected
          """
              .formatted(table, table);
      try (var query = db.prepareStatement(sql)) {
        query.setQueryTimeout(timeoutSeconds);
        query.setString(1, ">".equals(subject) ? "" : pattern);
        query.setString(2, pattern);
        query.setString(3, realmPrefix);
        query.setString(4, realmPrefix);
        try (var result = query.executeQuery()) {
          result.next();
          report.put("pending", result.getLong(1));
          report.put("oldest_age_seconds", result.getDouble(2));
          report.put("max_attempts", result.getLong(3));
          report.put("due", result.getLong(4));
          report.put("blocked_events", result.getLong(5));
          report.put("blocked_users", result.getLong(6));
          report.put("retrying_heads", result.getLong(7));
          report.put("publication_unknown", result.getLong(8));
          report.put("collected_at_timestamp_seconds", result.getDouble(9));
        }
      }
      String auditSql =
          """
          SELECT count(*),
            count(*) FILTER (WHERE reason = 'EXPIRED'),
            count(*) FILTER (WHERE reason = 'MAX_FAILURES'),
            count(*) FILTER (WHERE publication_may_have_occurred),
            count(*) FILTER (WHERE discarded_at <= extract(epoch FROM transaction_timestamp())*1000 - ?),
            greatest(0,coalesce(extract(epoch FROM transaction_timestamp()) -
              min(discarded_at) FILTER (WHERE discarded_at <= extract(epoch FROM transaction_timestamp())*1000 - ?)/1000.0,0))
          FROM %s WHERE (? = '' OR subject ~ ?) AND (? = '' OR starts_with(subject,?))
          """
              .formatted(table(schema, "kc_nats_discard_audit"));
      try (var query = db.prepareStatement(auditSql)) {
        query.setQueryTimeout(timeoutSeconds);
        query.setLong(1, auditRetentionSeconds * 1000L);
        query.setLong(2, auditRetentionSeconds * 1000L);
        query.setString(3, ">".equals(subject) ? "" : pattern);
        query.setString(4, pattern);
        query.setString(5, realmPrefix);
        query.setString(6, realmPrefix);
        try (var result = query.executeQuery()) {
          result.next();
          report.put("audit_retained", result.getLong(1));
          report.put("audit_retained_expired", result.getLong(2));
          report.put("audit_retained_max_failures", result.getLong(3));
          report.put("audit_retained_unknown", result.getLong(4));
          report.put("audit_eligible", result.getLong(5));
          report.put("audit_oldest_eligible_age_seconds", result.getDouble(6));
        }
      }
      try (var query =
          db.prepareStatement(
              """
              SELECT pg_total_relation_size(?::regclass),
                coalesce(n_dead_tup,0),coalesce(extract(epoch FROM last_autovacuum),0)
              FROM pg_stat_user_tables WHERE schemaname=? AND relname='kc_nats_outbox'
              """)) {
        query.setQueryTimeout(timeoutSeconds);
        query.setString(1, table);
        query.setString(2, schema);
        try (var result = query.executeQuery()) {
          if (!result.next()) {
            throw new IllegalStateException("Outbox table statistics are unavailable");
          }
          report.put("table_bytes", result.getLong(1));
          report.put("dead_rows_estimate", result.getLong(2));
          report.put("last_autovacuum_timestamp_seconds", result.getDouble(3));
        }
      }
      db.commit();
    }
    report.put("scrape_success", 1);
    return report;
  }

  /** Metadata-only keyset page, independent of NATS and the optional example application. */
  public static List<Map<String, Object>> inspect(
      DataSource database,
      String schema,
      int timeoutSeconds,
      long afterDiscardedAt,
      String afterId,
      int limit)
      throws Exception {
    if (afterId == null || limit < 1 || limit > 500) {
      throw new IllegalArgumentException("Expected a cursor and limit of 1..500");
    }
    String sql =
        """
        SELECT id, realm_id, event_type, subject, payload_sha256, ordering_key, user_sequence,
          max_age_seconds, max_failures, expires_at, filter_sha256, rule_id, created_at,
          discarded_at, reason, attempts, publication_may_have_occurred
        FROM %s WHERE (discarded_at, id) > (?, ?) ORDER BY discarded_at, id LIMIT ?
        """
            .formatted(table(schema, "kc_nats_discard_audit"));
    var rows = new ArrayList<Map<String, Object>>();
    try (var db = open(database, timeoutSeconds);
        var query = db.prepareStatement(sql)) {
      query.setQueryTimeout(timeoutSeconds);
      query.setLong(1, afterDiscardedAt);
      query.setString(2, afterId);
      query.setInt(3, limit);
      try (var result = query.executeQuery()) {
        while (result.next()) {
          var row = new LinkedHashMap<String, Object>();
          for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
            row.put(result.getMetaData().getColumnLabel(column), result.getObject(column));
          }
          rows.add(row);
        }
      }
      db.commit();
    }
    return rows;
  }

  private static String table(String schema, String name) {
    if (schema == null
        || schema.isBlank()
        || schema.indexOf('\0') >= 0
        || schema.getBytes(StandardCharsets.UTF_8).length > 63) {
      throw new IllegalArgumentException("Invalid report schema");
    }
    return "\"" + schema.replace("\"", "\"\"") + "\".\"" + name + "\"";
  }

  private static Connection open(DataSource database, int timeoutSeconds) throws Exception {
    if (timeoutSeconds < 1 || timeoutSeconds > 300) {
      throw new IllegalArgumentException("Invalid report timeout");
    }
    var db = database.getConnection();
    try {
      db.setReadOnly(true);
      db.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      db.setAutoCommit(false);
      try (var settings =
          db.prepareStatement(
              "SELECT set_config('statement_timeout', ?, true),"
                  + " set_config('lock_timeout', '1000', true)")) {
        settings.setString(1, Integer.toString(timeoutSeconds * 1000));
        settings.setQueryTimeout(timeoutSeconds);
        settings.execute();
      }
      return db;
    } catch (Exception failure) {
      db.close();
      throw failure;
    }
  }

  /** Fixed metric names keep event/realm cardinality under the monitoring owner's control. */
  public static String prometheus(Map<String, Number> report) {
    var output = new StringBuilder();
    report.forEach(
        (name, value) ->
            output.append("knd_outbox_").append(name).append(' ').append(value).append('\n'));
    return output.toString();
  }

  static String sqlPattern(String subject) {
    new SubjectPattern(subject);
    var expression = new StringBuilder("^");
    String[] tokens = subject.split("\\.");
    for (int i = 0; i < tokens.length; i++) {
      if (i > 0) {
        expression.append("\\.");
      }
      if ("*".equals(tokens[i])) {
        expression.append("[^.]+");
      } else if (">".equals(tokens[i])) {
        expression.append("[^.]+(\\.[^.]+)*");
      } else {
        for (char character : tokens[i].toCharArray()) {
          if ("\\^$|?+()[]{}".indexOf(character) >= 0) {
            expression.append('\\');
          }
          expression.append(character);
        }
      }
    }
    return expression.append('$').toString();
  }
}
