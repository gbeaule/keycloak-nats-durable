package io.github.keycloaknats;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import org.keycloak.Config;

/** Immutable, validated settings. Never render credentials in logs. */
public record BridgeConfig(
    String[] servers,
    String stream,
    String subjectPrefix,
    int minReplicas,
    Duration timeout,
    Duration pollInterval,
    Duration idlePollMax,
    int batchSize,
    Duration retryInitial,
    Duration retryMax,
    int maxPayloadBytes,
    String credentialsFile,
    String token) {
  /** Reads provider settings, falling back to matching environment variables. */
  public static BridgeConfig from(Config.Scope scope) {
    return read(key -> scope.get(key, System.getenv(envName(key))));
  }

  /** Reads explicit settings with the same validation and defaults as provider configuration. */
  public static BridgeConfig from(Map<String, String> values) {
    return read(values::get);
  }

  private static BridgeConfig read(Function<String, String> input) {
    Function<String, String> get =
        key -> {
          String value = input.apply(key);
          return value == null || value.isBlank() ? null : value.trim();
        };
    int pollMillis = number(get, "poll-ms", 500);
    return new BridgeConfig(
        value(get, "nats-url", "nats://localhost:4222").split(",", -1),
        value(get, "stream", "KEYCLOAK_EVENTS"),
        value(get, "subject-prefix", "keycloak.events"),
        number(get, "min-replicas", 3),
        Duration.ofMillis(number(get, "timeout-ms", 2000)),
        Duration.ofMillis(pollMillis),
        Duration.ofMillis(number(get, "idle-poll-max-ms", Math.max(pollMillis, 5000))),
        number(get, "batch-size", 64),
        Duration.ofMillis(number(get, "retry-initial-ms", 1000)),
        Duration.ofMillis(number(get, "retry-max-ms", 60000)),
        number(get, "max-payload-bytes", 65536),
        get.apply("credentials-file"),
        get.apply("token"));
  }

  /** Validates all limits and defensively copies the server list. */
  public BridgeConfig {
    if (servers == null || servers.length == 0) {
      throw new IllegalArgumentException("At least one NATS server is required");
    }
    servers = servers.clone();
    for (int i = 0; i < servers.length; i++) {
      servers[i] = validateServer(servers[i]);
    }
    if (stream == null || !stream.matches("[A-Za-z0-9_-]{1,128}")) {
      throw new IllegalArgumentException("Invalid stream name");
    }
    if (subjectPrefix == null
        || subjectPrefix.length() > 128
        || !subjectPrefix.matches("[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*")) {
      throw new IllegalArgumentException("Invalid subject prefix");
    }
    if (minReplicas < 1 || minReplicas > 5) {
      throw new IllegalArgumentException("min-replicas must be 1..5");
    }
    if (batchSize < 1 || batchSize > 1000) {
      throw new IllegalArgumentException("batch-size must be 1..1000");
    }
    if (maxPayloadBytes < 1024 || maxPayloadBytes > 1048576) {
      throw new IllegalArgumentException("max-payload-bytes must be 1024..1048576");
    }
    for (Duration duration :
        Arrays.asList(timeout, pollInterval, idlePollMax, retryInitial, retryMax)) {
      if (duration == null || duration.toMillis() < 1 || duration.toMillis() > 3600000) {
        throw new IllegalArgumentException("Durations must be 1..3600000 milliseconds");
      }
    }
    if (idlePollMax.compareTo(pollInterval) < 0) {
      throw new IllegalArgumentException("idle-poll-max-ms must be >= poll-ms");
    }
    if (retryMax.compareTo(retryInitial) < 0) {
      throw new IllegalArgumentException("retry-max-ms must be >= retry-initial-ms");
    }
    if (credentialsFile != null && token != null) {
      throw new IllegalArgumentException("Choose credentials-file or token, not both");
    }
  }

  private static String validateServer(String server) {
    if (server == null || server.isBlank()) {
      throw new IllegalArgumentException("NATS server entries must not be empty");
    }
    String address = server.trim();
    URI uri;
    try {
      uri = URI.create(address);
    } catch (IllegalArgumentException invalidUri) {
      // URI exceptions may contain inline credentials. Keep the diagnostic independent of input.
      throw new IllegalArgumentException("Invalid NATS server URI");
    }
    boolean supportedScheme = "nats".equals(uri.getScheme()) || "tls".equals(uri.getScheme());
    boolean hasHost = uri.getHost() != null;
    boolean hasInlineCredentials = uri.getUserInfo() != null;
    boolean hasQueryOrFragment = uri.getQuery() != null || uri.getFragment() != null;
    boolean hasPath = uri.getPath() != null && !uri.getPath().isEmpty();
    boolean validPort = uri.getPort() == -1 || (uri.getPort() >= 1 && uri.getPort() <= 65535);
    if (!supportedScheme
        || !hasHost
        || hasInlineCredentials
        || hasQueryOrFragment
        || hasPath
        || !validPort) {
      throw new IllegalArgumentException(
          "Use nats://host:port or tls://host:port; use separate credentials");
    }
    return address;
  }

  @Override
  public String[] servers() {
    return servers.clone();
  }

  @Override
  public String toString() {
    return "BridgeConfig[stream=" + stream + ", subjectPrefix=" + subjectPrefix + "]";
  }

  /** Maps a provider key such as {@code poll-ms} to {@code KND_POLL_MS}. */
  public static String envName(String key) {
    return "KND_" + key.replace('-', '_').toUpperCase(java.util.Locale.ROOT);
  }

  private static String value(Function<String, String> get, String key, String fallback) {
    String v = get.apply(key);
    return v == null ? fallback : v;
  }

  private static int number(Function<String, String> get, String key, int fallback) {
    try {
      return Integer.parseInt(value(get, key, Integer.toString(fallback)));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer");
    }
  }
}
