package io.github.keycloaknats;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

abstract class IntegrationSupport {
  static final ObjectMapper JSON = new ObjectMapper();
  static final String STREAM = "KEYCLOAK_EVENTS", DURABLE = "auth-worker";
  static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  static Network network;
  static PostgreSQLContainer postgres;
  static GenericContainer<?> broker, keycloak;
  static Connection nats;
  static PGSimpleDataSource database;
  static String adminToken;

  static void startInfrastructure() throws Exception {
    network = Network.newNetwork();
    postgres =
        new PostgreSQLContainer("postgres:17.6-alpine")
            .withDatabaseName("keycloak")
            .withUsername("keycloak")
            .withPassword("integration-password")
            .withNetwork(network)
            .withNetworkAliases("postgres");
    broker =
        new GenericContainer<>("nats:2.12.8-alpine")
            .withNetwork(network)
            .withNetworkAliases("nats")
            .withExposedPorts(4222)
            .withCopyFileToContainer(
                MountableFile.forClasspathResource("nats.conf"), "/etc/nats/test.conf")
            .withCommand("-c", "/etc/nats/test.conf");
    postgres.start();
    broker.start();
    database = new PGSimpleDataSource();
    database.setURL(postgres.getJdbcUrl());
    database.setUser(postgres.getUsername());
    database.setPassword(postgres.getPassword());
    connectNats();
    provision();
    keycloak = keycloakContainer(true);
    try {
      keycloak.start();
    } finally {
      saveLogs();
    }
    loginAdmin();
  }

  static GenericContainer<?> keycloakContainer(boolean importRealm) {
    var container =
        new GenericContainer<>(
                "quay.io/keycloak/keycloak:" + System.getProperty("keycloak.version", "26.7.4"))
            .withNetwork(network)
            .withExposedPorts(8080)
            .withEnv(
                Map.ofEntries(
                    Map.entry("KC_DB", "postgres"),
                    Map.entry("KC_DB_URL", "jdbc:postgresql://postgres:5432/keycloak"),
                    Map.entry("KC_DB_USERNAME", "keycloak"),
                    Map.entry("KC_DB_PASSWORD", "integration-password"),
                    Map.entry("KC_BOOTSTRAP_ADMIN_USERNAME", "admin"),
                    Map.entry("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin-password"),
                    Map.entry("KND_NATS_URL", "nats://nats:4222"),
                    Map.entry("KND_MIN_REPLICAS", "1"),
                    Map.entry("KND_TIMEOUT_MS", "500"),
                    Map.entry("KND_POLL_MS", "50"),
                    Map.entry("KND_RETRY_INITIAL_MS", "100"),
                    Map.entry("KND_RETRY_MAX_MS", "1000")))
            .withCopyFileToContainer(
                MountableFile.forHostPath(
                    Path.of(System.getProperty("extension.jar")).toAbsolutePath()),
                "/opt/keycloak/providers/nats-durable.jar")
            .waitingFor(Wait.forHttp("/realms/durable-test").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(3));
    if (importRealm)
      container.withCopyFileToContainer(
          MountableFile.forClasspathResource("realm.json"), "/opt/keycloak/data/import/realm.json");
    return container.withCommand("start-dev", "--import-realm", "--cache=ispn");
  }

  static void connectNats() throws Exception {
    if (nats != null) nats.close();
    nats =
        Nats.connect(
            new Options.Builder()
                .server(natsUrl())
                .maxReconnects(-1)
                .errorListener(new io.nats.client.ErrorListener() {})
                .connectionTimeout(Duration.ofSeconds(2))
                .build());
  }

  static int currentPort(GenericContainer<?> container, int port) {
    // Docker may reassign an ephemeral host port after a hard stop/start. Testcontainers caches the
    // original mapping.
    return Integer.parseInt(
        container
            .getDockerClient()
            .inspectContainerCmd(container.getContainerId())
            .exec()
            .getNetworkSettings()
            .getPorts()
            .getBindings()
            .get(new com.github.dockerjava.api.model.ExposedPort(port))[0]
            .getHostPortSpec());
  }

  static String natsUrl() {
    return "nats://" + broker.getHost() + ":" + currentPort(broker, 4222);
  }

  static void provision() throws Exception {
    nats.jetStreamManagement()
        .addStream(
            StreamConfiguration.builder()
                .name(STREAM)
                .subjects("keycloak.events.>")
                .storageType(StorageType.File)
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .replicas(1)
                .maxBytes(16777216)
                .duplicateWindow(Duration.ofMinutes(2))
                .build());
    consumer();
  }

  static void consumer() throws Exception {
    nats.jetStreamManagement()
        .addOrUpdateConsumer(
            STREAM,
            ConsumerConfiguration.builder()
                .durable(DURABLE)
                .filterSubject("keycloak.events.>")
                .deliverPolicy(DeliverPolicy.All)
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(Duration.ofMillis(400))
                .maxDeliver(-1)
                .build());
  }

  static String base() {
    return "http://" + keycloak.getHost() + ":" + currentPort(keycloak, 8080);
  }

  static HttpResponse<String> request(String method, String path, Object body) throws Exception {
    return HTTP.send(
        HttpRequest.newBuilder(URI.create(base() + path))
            .timeout(Duration.ofSeconds(15))
            .header("Authorization", "Bearer " + adminToken)
            .header("Content-Type", "application/json")
            .method(
                method,
                body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  static HttpResponse<String> login(String realm, String form) throws Exception {
    return HTTP.send(
        HttpRequest.newBuilder(
                URI.create(base() + "/realms/" + realm + "/protocol/openid-connect/token"))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  static void loginAdmin() throws Exception {
    var result =
        login(
            "master",
            "grant_type=password&client_id=admin-cli&username=admin&password=admin-password");
    assertEquals(200, result.statusCode(), result.body());
    adminToken = JSON.readTree(result.body()).get("access_token").asText();
  }

  static String createUser() throws Exception {
    var result =
        request(
            "POST",
            "/admin/realms/durable-test/users",
            Map.of("username", "user-" + UUID.randomUUID(), "enabled", true));
    assertEquals(201, result.statusCode(), result.body());
    String location = result.headers().firstValue("Location").orElseThrow();
    return location.substring(location.lastIndexOf('/') + 1);
  }

  static void execute(String sql) throws Exception {
    try (var connection = database.getConnection();
        var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  static long scalar(String sql) throws Exception {
    try (var connection = database.getConnection();
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      result.next();
      return result.getLong(1);
    }
  }

  static void drained() {
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> scalar("SELECT count(*) FROM kc_nats_outbox") == 0);
  }

  static long messages() throws Exception {
    return nats.jetStreamManagement().getStreamInfo(STREAM).getStreamState().getMsgCount();
  }

  static void restartKeycloak() throws Exception {
    var docker = keycloak.getDockerClient();
    docker.killContainerCmd(keycloak.getContainerId()).withSignal("KILL").exec();
    docker.startContainerCmd(keycloak.getContainerId()).exec();
    await()
        .atMost(Duration.ofMinutes(2))
        .ignoreExceptions()
        .until(
            () ->
                HTTP.send(
                            HttpRequest.newBuilder(URI.create(base() + "/realms/durable-test"))
                                .timeout(Duration.ofSeconds(2))
                                .GET()
                                .build(),
                            HttpResponse.BodyHandlers.discarding())
                        .statusCode()
                    == 200);
    loginAdmin();
  }

  static void saveLogs() throws Exception {
    Files.createDirectories(Path.of("target"));
    if (keycloak != null && keycloak.getContainerId() != null)
      Files.writeString(Path.of("target/keycloak.log"), keycloak.getLogs());
    if (broker != null && broker.getContainerId() != null)
      Files.writeString(Path.of("target/nats.log"), broker.getLogs());
  }

  static void stopInfrastructure() throws Exception {
    try {
      saveLogs();
    } finally {
      if (nats != null) nats.close();
      if (keycloak != null) keycloak.stop();
      if (broker != null) broker.stop();
      if (postgres != null) postgres.stop();
      if (network != null) network.close();
    }
  }
}
