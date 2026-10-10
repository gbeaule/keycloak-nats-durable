package io.github.gbeaule.keycloaknats;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

/** Shared fixture credentials; renewal never hides rejection of an unexpired token. */
final class AdminAuthentication {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Duration REQUEST_MARGIN = Duration.ofSeconds(15);
  private final Clock clock;
  private final Login login;
  private volatile Credentials credentials;

  @FunctionalInterface
  interface Login {
    String token() throws Exception;
  }

  @FunctionalInterface
  interface Request {
    HttpResponse<String> send(String token) throws Exception;
  }

  private record Credentials(String token, Instant expiresAt, Instant renewAt) {
    boolean expired(Instant now) {
      return !now.isBefore(expiresAt);
    }
  }

  AdminAuthentication(Clock clock, Login login) {
    this.clock = clock;
    this.login = login;
  }

  synchronized void login() throws Exception {
    String token = login.token();
    String[] parts = token.split("\\.", -1);
    if (parts.length != 3) {
      throw new IllegalArgumentException("Admin access token must contain a JWT expiry");
    }
    var expiration = objectMapper.readTree(Base64.getUrlDecoder().decode(parts[1])).path("exp");
    if (!expiration.isIntegralNumber() || !expiration.canConvertToLong()) {
      throw new IllegalArgumentException("Admin access token must contain an integer JWT expiry");
    }
    Instant now = clock.instant();
    Instant expiresAt = Instant.ofEpochSecond(expiration.longValue());
    if (!now.isBefore(expiresAt)) {
      throw new IllegalArgumentException("Admin login returned an expired access token");
    }
    Duration margin = Duration.between(now, expiresAt).dividedBy(4);
    if (margin.compareTo(REQUEST_MARGIN) > 0) {
      margin = REQUEST_MARGIN;
    }
    credentials = new Credentials(token, expiresAt, expiresAt.minus(margin));
  }

  HttpResponse<String> request(Request request) throws Exception {
    Credentials used = current();
    var response = request.send(used.token());
    if (response.statusCode() != 401 || !used.expired(clock.instant())) {
      return response;
    }
    // A request may cross exp in flight. Exactly one replay follows a proven expiry;
    // concurrent requests that used the same token share the replacement.
    synchronized (this) {
      if (credentials == used) {
        login();
      }
      used = credentials;
    }
    return request.send(used.token());
  }

  private Credentials current() throws Exception {
    Credentials current = credentials;
    if (current == null || !clock.instant().isBefore(current.renewAt())) {
      synchronized (this) {
        current = credentials;
        if (current == null || !clock.instant().isBefore(current.renewAt())) {
          login();
          current = credentials;
        }
      }
    }
    return current;
  }
}
