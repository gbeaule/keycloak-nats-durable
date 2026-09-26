package io.github.gbeaule.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.gbeaule.keycloaknats.tls.TlsConfig;
import io.nats.client.JetStreamOptions;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

/** Built optimized image, verified HTTPS, scoped publisher credentials, readiness and rotation. */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Maven Failsafe discovers the IT suffix.
class ProductionModeIT extends IntegrationSupport {
  @TempDir static Path certificatesDirectory;
  private static TestCertificates certificates;
  private static HttpClient secureClient;
  private static String image;
  private static TestCredentials credentials;

  @BeforeAll
  static void start() throws Exception {
    certificates = new TestCertificates(certificatesDirectory);
    credentials = new TestCredentials(certificatesDirectory);
    secureClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .sslContext(
                TlsConfig.from(
                        new String[] {"tls://localhost:4222"},
                        Map.of("tls-ca-file", certificates.file("ca.pem"))::get)
                    .createContext())
            .build();
    network = Network.newNetwork();
    postgres =
        postgresContainer()
            .withDatabaseName("keycloak")
            .withUsername("keycloak")
            .withPassword("integration-password")
            .withNetwork(network)
            .withNetworkAliases("postgres");
    broker =
        new GenericContainer<>(IntegrationSupport.NATS_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("nats")
            .withExposedPorts(4222)
            .withCopyToContainer(Transferable.of(credentials.serverConfig()), "/etc/nats/test.conf")
            .withCommand("-c", "/etc/nats/test.conf");
    postgres.start();
    broker.start();
    database = new PGSimpleDataSource();
    database.setURL(postgres.getJdbcUrl());
    database.setUser(postgres.getUsername());
    database.setPassword(postgres.getPassword());
    connectAdmin();
    provision();
    String version = System.getProperty("keycloak.version", "26.7.4");
    Path provider = Path.of(System.getProperty("extension.jar")).toAbsolutePath();
    String artifactVersion =
        provider.getFileName().toString().replace("keycloak-nats-durable-", "").replace(".jar", "");
    image =
        new ImageFromDockerfile("knd-production-test:" + UUID.randomUUID(), true)
            .withFileFromPath(
                "deploy/Dockerfile.keycloak", Path.of("../deploy/Dockerfile.keycloak"))
            .withFileFromPath("extension/target/" + provider.getFileName(), provider)
            .withDockerfilePath("deploy/Dockerfile.keycloak")
            .withBuildArg("ARTIFACT_VERSION", artifactVersion)
            .withBuildArg("KEYCLOAK_IMAGE", "quay.io/keycloak/keycloak:" + version)
            .get();
    keycloak = productionContainer(true);
    try {
      keycloak.start();
      loginSecureAdmin();
    } finally {
      saveLogs();
    }
  }

  @AfterAll
  static void stop() throws Exception {
    stopInfrastructure();
  }

  @Test
  void optimizedImagePublishesWithRestrictedPermissionsAndRotatesCredentials() throws Exception {
    assertTrue(keycloak.getLogs().contains("Profile prod activated"));
    assertFalse(keycloak.getLogs().contains("Running the server in development mode"));
    assertEquals(200, readiness());
    createSecureUser();
    drained();
    assertEquals(1, messages());

    var permissions = new AtomicInteger();
    try (var restricted =
        Nats.connect(
            new Options.Builder()
                .server(natsUrl())
                .authHandler(Nats.credentials(credentials.file("publisher")))
                .connectionTimeout(Duration.ofSeconds(2))
                .errorListener(
                    new io.nats.client.ErrorListener() {
                      @Override
                      public void errorOccurred(
                          io.nats.client.Connection connection, String error) {
                        permissions.incrementAndGet();
                      }
                    })
                .build())) {
      try {
        restricted
            .jetStreamManagement(
                JetStreamOptions.builder().requestTimeout(Duration.ofMillis(500)).build())
            .deleteStream(STREAM);
        throw new AssertionError("Publisher credentials must not delete a stream");
      } catch (java.io.IOException expected) {
        await().atMost(Duration.ofSeconds(2)).until(() -> permissions.get() > 0);
      }
      assertEquals(1, messages());
    }

    var docker = broker.getDockerClient();
    docker.stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    try {
      createSecureUser();
      assertEquals(1, scalar("SELECT count(*) FROM kc_nats_outbox"));
      assertEquals(
          200, readiness(), "Broker failure must not remove healthy Keycloak from service");
    } finally {
      docker.startContainerCmd(broker.getContainerId()).exec();
    }
    connectAdmin();
    drained();
    assertEquals(2, messages());

    final String oldCredentials = Files.readString(Path.of(credentials.file("publisher")));
    credentials.rotatePublisher();
    restrictBroker();
    createSecureUser();
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> scalar("SELECT count(*) FROM kc_nats_outbox WHERE attempts>0") == 1);
    assertEquals(200, readiness());
    assertNoCredentialsInLogs(oldCredentials);
    keycloak.stop();
    keycloak = productionContainer(false);
    keycloak.start();
    connectAdmin();
    drained();
    assertEquals(3, messages());
    assertNoCredentialsInLogs(Files.readString(Path.of(credentials.file("publisher"))));
    Files.writeString(Path.of("target/production-keycloak.log"), keycloak.getLogs());
  }

  private static GenericContainer<?> productionContainer(boolean importRealm) {
    return configureKeycloakContainer(new GenericContainer<>(image), importRealm, false)
        .withExposedPorts(8443, 9000)
        .withEnv("KND_CREDENTIALS_FILE", "/opt/keycloak/conf/publisher.creds")
        .withEnv("KND_RELAY_WORKERS", "4")
        .withCopyFileToContainer(
            MountableFile.forHostPath(credentials.file("publisher")),
            "/opt/keycloak/conf/publisher.creds")
        .withCopyFileToContainer(
            MountableFile.forHostPath(certificates.file("server.pem")),
            "/opt/keycloak/conf/server.pem")
        .withCopyFileToContainer(
            MountableFile.forHostPath(certificates.file("server.key")),
            "/opt/keycloak/conf/server.key")
        .withCommand(
            "start",
            "--optimized",
            "--import-realm",
            "--hostname=https://localhost:8443",
            "--https-certificate-file=/opt/keycloak/conf/server.pem",
            "--https-certificate-key-file=/opt/keycloak/conf/server.key",
            "--http-management-scheme=http")
        .waitingFor(Wait.forHttp("/health/ready").forPort(9000).forStatusCode(200));
  }

  private static void assertNoCredentialsInLogs(String contents) {
    contents
        .lines()
        .filter(line -> !line.isBlank() && !line.startsWith("---"))
        .forEach(line -> assertFalse(keycloak.getLogs().contains(line)));
  }

  private static void restrictBroker() throws Exception {
    broker.copyFileToContainer(Transferable.of(credentials.serverConfig()), "/etc/nats/test.conf");
    // The disposable MEMORY resolver loads account claims at startup. Restart also forces every
    // established connection to prove its identity again against the revocation list.
    broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(1).exec();
    broker.getDockerClient().startContainerCmd(broker.getContainerId()).exec();
    connectAdmin();
    await()
        .atMost(Duration.ofSeconds(10))
        .ignoreExceptions()
        .until(
            () -> {
              try (var connection =
                  Nats.connect(
                      new Options.Builder()
                          .server(natsUrl())
                          .authHandler(Nats.credentials(credentials.file("publisher")))
                          .connectionTimeout(Duration.ofSeconds(1))
                          .errorListener(new io.nats.client.ErrorListener() {})
                          .build())) {
                return connection.getStatus() == io.nats.client.Connection.Status.CONNECTED;
              }
            });
  }

  private static void connectAdmin() throws Exception {
    if (nats != null) {
      nats.close();
    }
    nats =
        Nats.connect(
            new Options.Builder()
                .server(natsUrl())
                .authHandler(Nats.credentials(credentials.file("admin")))
                .maxReconnects(-1)
                .connectionTimeout(Duration.ofSeconds(2))
                .errorListener(new io.nats.client.ErrorListener() {})
                .build());
  }

  private static void loginSecureAdmin() throws Exception {
    var response =
        secureClient.send(
            HttpRequest.newBuilder(secureUri("/realms/master/protocol/openid-connect/token"))
                .timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        "grant_type=password&client_id=admin-cli"
                            + "&username=admin&password=admin-password"))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode());
    adminToken = objectMapper.readTree(response.body()).path("access_token").asText();
  }

  private static void createSecureUser() throws Exception {
    var response =
        secureClient.send(
            HttpRequest.newBuilder(secureUri("/admin/realms/durable-test/users"))
                .timeout(Duration.ofSeconds(15))
                .header("Authorization", "Bearer " + adminToken)
                .header("Content-Type", "application/json")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        objectMapper.writeValueAsString(
                            Map.of(
                                "username", "production-" + UUID.randomUUID(), "enabled", true))))
                .build(),
            HttpResponse.BodyHandlers.discarding());
    assertEquals(201, response.statusCode());
  }

  private static URI secureUri(String path) {
    return URI.create("https://localhost:" + keycloak.getMappedPort(8443) + path);
  }

  private static int readiness() throws Exception {
    return httpClient
        .send(
            HttpRequest.newBuilder(
                    URI.create(
                        "http://"
                            + keycloak.getHost()
                            + ":"
                            + keycloak.getMappedPort(9000)
                            + "/health/ready"))
                .timeout(Duration.ofSeconds(2))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.discarding())
        .statusCode();
  }
}
