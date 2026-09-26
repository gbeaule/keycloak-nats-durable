package io.github.gbeaule.keycloaknats.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gbeaule.keycloaknats.routing.SubjectPattern;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
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
      if (args.length > 1 || (args.length == 1 && !prometheus && !"json".equals(args[0]))) {
        throw new IllegalArgumentException("Expected json or prometheus");
      }
      var settings = ConsumerConfig.system().report();
      Map<String, Number> report =
          collect(
              settings.database(),
              settings.schema(),
              settings.subject(),
              settings.realmPrefix(),
              settings.timeoutSeconds());
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
    if (schema == null
        || schema.isBlank()
        || schema.indexOf('\0') >= 0
        || schema.getBytes(StandardCharsets.UTF_8).length > 63
        || timeoutSeconds < 1
        || timeoutSeconds > 300) {
      throw new IllegalArgumentException("Invalid report schema or timeout");
    }
    String table = "\"" + schema.replace("\"", "\"\"") + "\".\"kc_nats_outbox\"";
    String pattern = sqlPattern(subject);
    var report = new LinkedHashMap<String, Number>();
    try (var db = database.getConnection()) {
      db.setReadOnly(true);
      db.setAutoCommit(false);
      try (var settings =
          db.prepareStatement(
              "SELECT set_config('statement_timeout', ?, true),"
                  + " set_config('lock_timeout', '1000', true)")) {
        settings.setString(1, Integer.toString(timeoutSeconds * 1000));
        settings.setQueryTimeout(timeoutSeconds);
        settings.execute();
      }
      String sql =
          """
          SELECT count(*),
            greatest(0,coalesce(extract(epoch FROM clock_timestamp())-min(created_at)/1000.0,0)),
            coalesce(max(attempts),0),
            count(*) FILTER (WHERE next_attempt_at <= extract(epoch FROM clock_timestamp())*1000)
          FROM %s WHERE (? = '' OR subject ~ ?) AND (? = '' OR starts_with(subject,?))
          """
              .formatted(table);
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
    report.put("collected_at_timestamp_seconds", Instant.now().getEpochSecond());
    report.put("scrape_success", 1);
    return report;
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
