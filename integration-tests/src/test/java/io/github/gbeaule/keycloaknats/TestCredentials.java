package io.github.gbeaule.keycloaknats;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.NKey;
import io.nats.client.support.JwtUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Disposable operator/account/user credentials; no secrets are checked into the repository. */
final class TestCredentials {
  private final SecureRandom random = new SecureRandom();
  private final NKey operator = NKey.createOperator(random);
  private final NKey account = NKey.createAccount(random);
  private final NKey system = NKey.createAccount(random);
  private final Path directory;
  private final Map<String, Long> revoked = new HashMap<>();
  private NKey publisher;
  private long issued;

  TestCredentials(Path directory) throws Exception {
    this.directory = directory;
    writeUser(NKey.createUser(random), "admin", new JwtUtils.UserClaim(publicKey(account)));
    rotatePublisher();
  }

  String file(String name) {
    return directory.resolve(name + ".creds").toString();
  }

  void rotatePublisher() throws Exception {
    if (publisher != null) {
      revoked.put(publicKey(publisher), JwtUtils.currentTimeSeconds());
    }
    publisher = NKey.createUser(random);
    writeUser(
        publisher,
        "publisher",
        new JwtUtils.UserClaim(publicKey(account))
            .pub(
                new JwtUtils.Permission()
                    .allow("keycloak.events.>", "$JS.API.STREAM.INFO.KEYCLOAK_EVENTS"))
            .sub(new JwtUtils.Permission().allow("_INBOX.>")));
  }

  String serverConfig() throws Exception {
    issued = Math.max(JwtUtils.currentTimeSeconds(), issued + 1);
    String operatorJwt =
        claim(
            operator,
            operator,
            Map.of("type", "operator", "version", 2, "system_account", publicKey(system)));
    String accountJwt =
        claim(
            operator,
            account,
            Map.of(
                "type",
                "account",
                "version",
                2,
                "revocations",
                revoked,
                "limits",
                Map.of(
                    "subs", -1,
                    "data", -1,
                    "payload", -1,
                    "conn", -1,
                    "mem_storage", -1,
                    "disk_storage", -1,
                    "streams", -1,
                    "consumer", -1)));
    String systemJwt = claim(operator, system, Map.of("type", "account", "version", 2));
    return "port: 4222\njetstream { store_dir: /data/jetstream, sync_interval: always }\n"
        + "operator: \""
        + operatorJwt
        + "\"\nresolver: MEMORY\nresolver_preload {\n"
        + publicKey(account)
        + ": \""
        + accountJwt
        + "\"\n"
        + publicKey(system)
        + ": \""
        + systemJwt
        + "\"\n}\n";
  }

  private String claim(NKey signer, NKey subject, Map<String, Object> body) throws Exception {
    String json = new ObjectMapper().writeValueAsString(body);
    return JwtUtils.issueJWT(
        signer,
        publicKey(subject),
        "integration",
        Duration.ofHours(2),
        issued,
        publicKey(signer),
        () -> json);
  }

  private void writeUser(NKey user, String name, JwtUtils.UserClaim permissions) throws Exception {
    String jwt =
        JwtUtils.issueUserJWT(
            account,
            publicKey(user),
            name,
            Duration.ofHours(2),
            JwtUtils.currentTimeSeconds(),
            permissions);
    Files.writeString(
        Path.of(file(name)),
        "-----BEGIN NATS USER JWT-----\n"
            + jwt
            + "\n------END NATS USER JWT------\n\n"
            + "-----BEGIN USER NKEY SEED-----\n"
            + new String(user.getSeed())
            + "\n------END USER NKEY SEED------\n");
  }

  private static String publicKey(NKey key) throws Exception {
    return new String(key.getPublicKey());
  }
}
