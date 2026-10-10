package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class AdminAuthenticationTest {
  private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void concurrentRequestsRenewBeforeExpiryOnceAndNeverSendTheOldToken() throws Exception {
    var clock = new MutableClock();
    var logins = new AtomicInteger();
    var auth = authentication(clock, logins);
    auth.login();
    clock.now.set(START.plusSeconds(50));
    var tokens = ConcurrentHashMap.<String>newKeySet();
    var success = response(201);
    var start = new CountDownLatch(1);
    try (var workers = Executors.newFixedThreadPool(16)) {
      var work = new ArrayList<java.util.concurrent.Future<?>>();
      for (int i = 0; i < 16; i++) {
        work.add(
            workers.submit(
                () -> {
                  assertTrue(start.await(5, TimeUnit.SECONDS));
                  assertSame(
                      success,
                      auth.request(
                          token -> {
                            tokens.add(token);
                            return success;
                          }));
                  return null;
                }));
      }
      start.countDown();
      for (var request : work) {
        request.get(5, TimeUnit.SECONDS);
      }
    }
    assertEquals(2, logins.get());
    assertEquals(java.util.Set.of(token(START.plusSeconds(110), 2)), tokens);
  }

  @Test
  void rejectionOfAnUnexpiredTokenRemainsVisibleWithoutRetry() throws Exception {
    var clock = new MutableClock();
    var logins = new AtomicInteger();
    var auth = authentication(clock, logins);
    var sent = new AtomicInteger();
    var rejection = response(401);
    assertSame(
        rejection,
        auth.request(
            token -> {
              sent.incrementAndGet();
              return rejection;
            }));
    assertEquals(1, sent.get());
    assertEquals(1, logins.get());
  }

  @Test
  void concurrentInFlightExpirationsShareOneRenewalAndOneRetryPerRequest() throws Exception {
    var clock = new MutableClock();
    var logins = new AtomicInteger();
    var auth = authentication(clock, logins);
    auth.login();
    String old = token(START.plusSeconds(60), 1);
    var arrived = new CountDownLatch(16);
    var expire = new CountDownLatch(1);
    var attempts = new AtomicInteger();
    var rejection = response(401);
    var success = response(201);
    try (var workers = Executors.newFixedThreadPool(16)) {
      var work = new ArrayList<java.util.concurrent.Future<?>>();
      try {
        for (int i = 0; i < 16; i++) {
          work.add(
              workers.submit(
                  () -> {
                    assertSame(
                        success,
                        auth.request(
                            used -> {
                              attempts.incrementAndGet();
                              if (used.equals(old)) {
                                arrived.countDown();
                                assertTrue(expire.await(5, TimeUnit.SECONDS));
                                return rejection;
                              }
                              assertEquals(token(START.plusSeconds(120), 2), used);
                              return success;
                            }));
                    return null;
                  }));
        }
        assertTrue(arrived.await(5, TimeUnit.SECONDS));
        clock.now.set(START.plusSeconds(60));
        expire.countDown();
        for (var request : work) {
          request.get(5, TimeUnit.SECONDS);
        }
      } finally {
        expire.countDown();
      }
    }
    assertEquals(2, logins.get());
    assertEquals(32, attempts.get());
  }

  @Test
  void failedRetryIsReturnedWithoutAnotherAttempt() throws Exception {
    var clock = new MutableClock();
    var logins = new AtomicInteger();
    var auth = authentication(clock, logins);
    var attempts = new AtomicInteger();
    var rejection = response(401);
    assertSame(
        rejection,
        auth.request(
            token -> {
              attempts.incrementAndGet();
              clock.now.set(START.plusSeconds(60));
              return rejection;
            }));
    assertEquals(2, attempts.get());
    assertEquals(2, logins.get());
  }

  @Test
  void shortLivedTokensDoNotRefreshOnEveryRequest() throws Exception {
    var clock = new MutableClock();
    var logins = new AtomicInteger();
    var auth =
        new AdminAuthentication(
            clock, () -> token(clock.instant().plusSeconds(2), logins.incrementAndGet()));
    var success = response(201);
    auth.request(token -> success);
    clock.now.set(START.plusSeconds(1));
    auth.request(token -> success);
    assertEquals(1, logins.get());
    clock.now.set(START.plusMillis(1600));
    auth.request(token -> success);
    assertEquals(2, logins.get());
  }

  @Test
  void renewalFailureStopsTheRequestAndRemainsVisible() throws Exception {
    var clock = new MutableClock();
    var logins = new AtomicInteger();
    var auth =
        new AdminAuthentication(
            clock,
            () -> {
              if (logins.incrementAndGet() > 1) {
                throw new IOException("Token endpoint unavailable");
              }
              return token(START.plusSeconds(60), 1);
            });
    auth.login();
    clock.now.set(START.plusSeconds(50));
    var sent = new AtomicInteger();
    assertThrows(
        IOException.class,
        () ->
            auth.request(
                token -> {
                  sent.incrementAndGet();
                  return response(201);
                }));
    assertEquals(0, sent.get());
  }

  @ParameterizedTest
  @ValueSource(strings = {"opaque", "header.e30.signature", "header.eyJleHAiOjEuNX0.signature"})
  void tokensWithoutAnIntegerExpiryAreRejected(String token) {
    var auth = new AdminAuthentication(new MutableClock(), () -> token);
    assertThrows(IllegalArgumentException.class, auth::login);
  }

  @Test
  void alreadyExpiredLoginTokensAreRejected() {
    var auth = new AdminAuthentication(new MutableClock(), () -> token(START.minusSeconds(1), 1));
    assertThrows(IllegalArgumentException.class, auth::login);
  }

  private static AdminAuthentication authentication(MutableClock clock, AtomicInteger logins) {
    return new AdminAuthentication(
        clock, () -> token(clock.instant().plusSeconds(60), logins.incrementAndGet()));
  }

  private static String token(Instant expiry, int identity) {
    String payload = "{\"exp\":" + expiry.getEpochSecond() + ",\"jti\":" + identity + "}";
    return "header."
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8))
        + ".signature";
  }

  @SuppressWarnings("unchecked")
  private static HttpResponse<String> response(int status) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(status);
    return response;
  }

  private static final class MutableClock extends Clock {
    private final AtomicReference<Instant> now = new AtomicReference<>(START);

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now.get();
    }
  }
}
