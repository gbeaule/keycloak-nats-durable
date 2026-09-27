package io.github.gbeaule.keycloaknats.consumer;

import io.github.gbeaule.keycloaknats.config.Environment;
import io.github.gbeaule.keycloaknats.config.NatsServers;
import io.github.gbeaule.keycloaknats.routing.SubjectPattern;
import io.github.gbeaule.keycloaknats.tls.TlsConfig;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * All environment names and defaults for the example consumer, provisioner and backlog collector.
 */
public final class ConsumerConfig {
  private final Environment environment;

  /** Injects configuration without reading the process environment. */
  public ConsumerConfig(Function<String, String> values) {
    environment = new Environment(values);
  }

  /** Uses the shared environment boundary at the application's entry point. */
  public static ConsumerConfig system() {
    return new ConsumerConfig(Environment.system()::optional);
  }

  /** Validated worker settings; migrations and loss policies remain opt-in. */
  public ConsumerSettings settings() {
    String migrate = environment.optional("KND_CONSUMER_AUTO_MIGRATE");
    if (migrate != null && !"true".equals(migrate) && !"false".equals(migrate)) {
      throw new IllegalArgumentException("KND_CONSUMER_AUTO_MIGRATE must be true or false");
    }
    return new ConsumerSettings(
        processing(),
        failures(),
        environment.integer("KND_CONSUMER_ACK_WAIT_MS", 30000),
        environment.integer("KND_CONSUMER_ACK_TIMEOUT_MS", 5000),
        environment.integer("KND_CONSUMER_PROGRESS_MS", 5000),
        environment.integer("KND_CONSUMER_MAX_ACK_PENDING", 1000),
        environment.integer("KND_CONSUMER_MONITOR_SECONDS", 30),
        environment.value("KND_CONSUMER_HEALTH_BIND", "127.0.0.1"),
        environment.integer("KND_CONSUMER_HEALTH_PORT", 0),
        "true".equals(migrate));
  }

  /** Database and handler deadlines, independent of connection credentials. */
  public ProcessingLimits processing() {
    return new ProcessingLimits(
        environment.integer("KND_CONSUMER_CONNECT_SECONDS", 5),
        environment.integer("KND_CONSUMER_SOCKET_SECONDS", 15),
        environment.integer("KND_CONSUMER_STATEMENT_MS", 10000),
        environment.integer("KND_CONSUMER_LOCK_MS", 2000),
        environment.integer("KND_CONSUMER_DEADLINE_MS", 20000),
        environment.integer("KND_CONSUMER_CLEANUP_MS", 1000));
  }

  /** Explicit subject-scoped recovery; retry is the default. */
  public FailurePolicy failures() {
    String configured = environment.value("KND_CONSUMER_FAILURE_ACTION", "retry");
    FailurePolicy.Action action;
    try {
      action = FailurePolicy.Action.valueOf(configured.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException invalid) {
      throw new IllegalArgumentException("Invalid KND_CONSUMER_FAILURE_ACTION");
    }
    String patterns = environment.optional("KND_CONSUMER_FAILURE_SUBJECTS");
    List<SubjectPattern> subjects =
        patterns == null
            ? List.of()
            : Arrays.stream(patterns.split(",", -1))
                .map(String::strip)
                .map(SubjectPattern::new)
                .toList();
    return new FailurePolicy(
        action,
        environment.integer("KND_CONSUMER_FAILURE_MIN_DELIVERIES", 5),
        subjects,
        environment.integer("KND_CONSUMER_DROP_AFTER_SECONDS", 0));
  }

  /** Configures the example database without opening a connection. */
  public PGSimpleDataSource database(ProcessingLimits limits) {
    var database = new PGSimpleDataSource();
    database.setURL(
        environment.value("KND_CONSUMER_DB_URL", "jdbc:postgresql://localhost:5432/consumer"));
    database.setUser(environment.value("KND_CONSUMER_DB_USER", "consumer"));
    database.setPassword(environment.value("KND_CONSUMER_DB_PASSWORD", ""));
    limits.configure(database);
    return database;
  }

  /** Maximum reusable connections; additional pool capacity does not create workers. */
  public int poolSize() {
    return environment.integer(
        "KND_CONSUMER_DB_POOL_SIZE",
        1,
        ConsumerDatabase.MIN_POOL_SIZE,
        ConsumerDatabase.MAX_POOL_SIZE);
  }

  /** Destination stream, shared by provisioning and consumption. */
  public String stream() {
    return environment.value("KND_STREAM", "KEYCLOAK_EVENTS");
  }

  /** Subject prefix, shared with the provider's routing contract. */
  public String subjectPrefix() {
    return environment.value("KND_SUBJECT_PREFIX", "keycloak.events");
  }

  /** Durable name used for the broker subscription and database deduplication. */
  public String durable() {
    return environment.value("KND_CONSUMER", "auth-worker");
  }

  /** Required broker replicas when provisioning or validating a replay destination. */
  public int minReplicas() {
    return environment.integer("KND_MIN_REPLICAS", 3, 1, 5);
  }

  /** Stream capacity applied only during explicit provisioning. */
  public long streamMaxBytes() {
    return environment.longValue("KND_STREAM_MAX_BYTES", 1L << 30);
  }

  /** Reads transport settings only for commands that connect to NATS. */
  public NatsSettings nats() {
    String[] servers = environment.value("KND_NATS_URL", "nats://localhost:4222").split(",", -1);
    String credentials = environment.optional("KND_CREDENTIALS_FILE");
    String token = environment.optional("KND_TOKEN");
    if (credentials != null && token != null) {
      throw new IllegalArgumentException("Choose credentials-file or token, not both");
    }
    return new NatsSettings(
        servers,
        TlsConfig.from(servers, key -> environment.optional(Environment.name(key))),
        credentials,
        token);
  }

  /** Collector settings do not require example-worker or NATS configuration. */
  public ReportSettings report() {
    String url = environment.optional("KND_OUTBOX_DB_URL");
    if (url == null || url.isBlank()) {
      throw new IllegalArgumentException("KND_OUTBOX_DB_URL is required");
    }
    var database = new PGSimpleDataSource();
    database.setURL(url);
    database.setUser(environment.value("KND_OUTBOX_DB_USER", "knd_monitor"));
    database.setPassword(environment.value("KND_OUTBOX_DB_PASSWORD", ""));
    int timeout = environment.integer("KND_REPORT_TIMEOUT_SECONDS", 5, 1, 300);
    new ProcessingLimits(timeout, timeout, timeout * 1000, 1000, timeout * 1000, 1000)
        .configure(database);
    String realm = environment.optional("KND_REPORT_REALM_ID");
    String realmPrefix =
        realm == null
            ? ""
            : subjectPrefix()
                + "."
                + Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(realm.getBytes(StandardCharsets.UTF_8))
                + ".";
    return new ReportSettings(
        database,
        environment.value("KND_OUTBOX_DB_SCHEMA", "public"),
        environment.value("KND_REPORT_SUBJECT", ">"),
        realmPrefix,
        timeout,
        environment.integer("KND_REPORT_MAX_ROWS", 0, 0, Integer.MAX_VALUE),
        environment.integer("KND_REPORT_MAX_AGE_SECONDS", 0, 0, Integer.MAX_VALUE));
  }

  /** Connection configuration with credentials excluded from diagnostic rendering. */
  public record NatsSettings(
      String[] servers, TlsConfig tls, String credentialsFile, String token) {
    /** Validates and copies addresses before they can reach the NATS client. */
    public NatsSettings {
      servers = NatsServers.validate(servers);
      boolean allTls = Arrays.stream(servers).allMatch(server -> server.startsWith("tls://"));
      boolean anyTls = Arrays.stream(servers).anyMatch(server -> server.startsWith("tls://"));
      if (tls == null || anyTls != allTls || tls.enabled() != allTls) {
        throw new IllegalArgumentException("TLS settings must match every NATS server");
      }
    }

    @Override
    public String[] servers() {
      return servers.clone();
    }

    @Override
    public String toString() {
      return "NatsSettings[redacted]";
    }
  }

  /** Validated collector inputs; no connections are opened while loading configuration. */
  public record ReportSettings(
      PGSimpleDataSource database,
      String schema,
      String subject,
      String realmPrefix,
      int timeoutSeconds,
      long maxRows,
      long maxAgeSeconds) {
    @Override
    public String toString() {
      return "ReportSettings[redacted]";
    }
  }
}
