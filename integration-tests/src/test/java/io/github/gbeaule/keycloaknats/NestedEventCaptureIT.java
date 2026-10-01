package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Built-in password-update followed by email verification with unavailable SMTP. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class NestedEventCaptureIT extends IntegrationSupport {
  @org.junit.jupiter.api.Test
  void passwordSuccessAndNestedEmailErrorAreBothDurable() throws Exception {
    try {
      startInfrastructure(
          System.getProperty("keycloak.version", "26.7.4"),
          container -> {
            try {
              execute("ALTER ROLE keycloak SET lock_timeout = '1500ms'");
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          },
          false);
      var user =
          objectMapper
              .readTree(
                  request("GET", "/admin/realms/durable-test/users?username=alice", null).body())
              .get(0)
              .path("id")
              .asText();
      var client =
          objectMapper
              .readTree(
                  request("GET", "/admin/realms/durable-test/clients?clientId=test-client", null)
                      .body())
              .get(0)
              .path("id")
              .asText();
      var actions =
          objectMapper.readTree(
              request("GET", "/admin/realms/durable-test/authentication/required-actions", null)
                  .body());
      for (var item : actions) {
        String alias = item.path("alias").asText();
        if (alias.equals("UPDATE_PASSWORD") || alias.equals("VERIFY_EMAIL")) {
          ((com.fasterxml.jackson.databind.node.ObjectNode) item)
              .put("priority", alias.equals("UPDATE_PASSWORD") ? 0 : 1000);
          update("/admin/realms/durable-test/authentication/required-actions/" + alias, item);
        }
      }
      update(
          "/admin/realms/durable-test/users/" + user,
          Map.of(
              "emailVerified",
              false,
              "requiredActions",
              List.of("UPDATE_PASSWORD", "VERIFY_EMAIL")));
      update(
          "/admin/realms/durable-test/clients/" + client,
          Map.of(
              "standardFlowEnabled", true, "redirectUris", List.of("http://localhost/callback")));
      drained();
      nats.jetStreamManagement().purgeStream(STREAM);
      var cookies = new CookieManager(null, CookiePolicy.ACCEPT_ALL);
      var browser =
          HttpClient.newBuilder()
              .cookieHandler(cookies)
              .followRedirects(HttpClient.Redirect.NORMAL)
              .connectTimeout(Duration.ofSeconds(5))
              .build();
      var login =
          browser.send(
              HttpRequest.newBuilder(
                      URI.create(
                          "http://"
                              + keycloak.getHost()
                              + ":"
                              + keycloak.getMappedPort(8080)
                              + "/realms/durable-test/protocol/openid-connect/auth"
                              + "?client_id=test-client&response_type=code&scope=openid"
                              + "&redirect_uri=http%3A%2F%2Flocalhost%2Fcallback"))
                  .timeout(Duration.ofSeconds(15))
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      cookies
          .getCookieStore()
          .getCookies()
          .forEach(
              cookie -> {
                cookie.setVersion(0);
                cookie.setSecure(
                    false); // Match browsers' secure-cookie exception for localhost HTTP.
              });
      var passwordForm =
          post(
              browser,
              action(login.body()),
              Map.of("username", "alice", "password", "alice-password"));
      assertTrue(passwordForm.body().contains("password-new"), "No password form");
      cookies
          .getCookieStore()
          .getCookies()
          .forEach(
              cookie -> {
                cookie.setVersion(0);
                cookie.setSecure(false);
              });
      var result =
          post(
              browser,
              action(passwordForm.body()),
              Map.of(
                  "password-new",
                  "review-password-new",
                  "password-confirm",
                  "review-password-new"));
      assertEquals(500, result.statusCode(), "SMTP is intentionally unavailable");
      drained();
      var management = nats.jetStreamManagement();
      var state = management.getStreamInfo(STREAM).getStreamState();
      var types = new java.util.HashSet<String>();
      var sequences = new java.util.HashSet<String>();
      for (long seq = Math.max(1, state.getFirstSequence());
          seq <= state.getLastSequence();
          seq++) {
        var event =
            objectMapper.readTree(management.getMessage(STREAM, seq).getData()).path("data");
        assertEquals(user, event.path("userId").asText());
        types.add(event.path("eventType").asText());
        sequences.add(event.path("ordering").path("sequence").asText());
      }
      assertEquals(
          java.util.Set.of("UPDATE_PASSWORD", "UPDATE_CREDENTIAL", "SEND_VERIFY_EMAIL_ERROR"),
          types);
      assertEquals(3, sequences.size());
      assertFalse(keycloak.getLogs().contains("LockTimeoutException"));
    } finally {
      if (keycloak != null && keycloak.getContainerId() != null) {
        Files.writeString(
            java.nio.file.Path.of("target/nested-event-keycloak.log"), keycloak.getLogs());
      }
      stopInfrastructure();
    }
  }

  private static void update(String path, Object body) throws Exception {
    var response = request("PUT", path, body);
    assertEquals(204, response.statusCode(), response.body());
  }

  private static String action(String html) {
    var matcher = Pattern.compile("<form\\b[^>]*\\baction=\"([^\"]+)\"").matcher(html);
    assertTrue(matcher.find(), "No form action");
    return matcher.group(1).replace("&amp;", "&");
  }

  private static HttpResponse<String> post(
      HttpClient browser, String action, Map<String, String> fields) throws Exception {
    String body =
        fields.entrySet().stream()
            .map(
                e ->
                    URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8)
                        + "="
                        + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
            .collect(Collectors.joining("&"));
    return browser.send(
        HttpRequest.newBuilder(URI.create(action))
            .timeout(Duration.ofSeconds(20))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
