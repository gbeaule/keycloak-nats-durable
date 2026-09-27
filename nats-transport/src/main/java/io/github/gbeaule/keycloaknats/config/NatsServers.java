package io.github.gbeaule.keycloaknats.config;

import java.net.URI;

/** Shared validation for NATS seed addresses, with credentials kept out of diagnostics. */
public final class NatsServers {
  private NatsServers() {}

  /** Returns a normalized copy; empty entries and credentials embedded in URLs are rejected. */
  public static String[] validate(String[] servers) {
    if (servers == null || servers.length == 0) {
      throw new IllegalArgumentException("At least one NATS server is required");
    }
    String[] normalized = servers.clone();
    for (int i = 0; i < normalized.length; i++) {
      normalized[i] = validateServer(normalized[i]);
    }
    return normalized;
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
}
