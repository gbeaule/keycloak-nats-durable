package io.github.keycloaknats;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Generates short-lived test identities using the test JDK; no private keys are committed. */
final class TestCertificates {
  private static final String PASSWORD = "integration-only";
  private final Path directory;

  TestCertificates(Path directory) throws Exception {
    this.directory = directory;
    keytool(
        "-genkeypair",
        "-alias",
        "ca",
        "-dname",
        "CN=KND Test CA",
        "-keyalg",
        "RSA",
        "-keysize",
        "2048",
        "-validity",
        "2",
        "-ext",
        "BC=ca:true",
        "-ext",
        "KU=keyCertSign,cRLSign",
        "-keystore",
        file("ca.p12"));
    keytool(
        "-exportcert",
        "-alias",
        "ca",
        "-rfc",
        "-keystore",
        file("ca.p12"),
        "-file",
        file("ca.pem"));
    identity("server", "serverAuth,clientAuth", "SAN=dns:localhost,dns:nats1,dns:nats2,dns:nats3");
    identity("client", "clientAuth", "SAN=dns:keycloak");
  }

  String file(String name) {
    return directory.resolve(name).toAbsolutePath().toString();
  }

  private void identity(String alias, String usage, String san) throws Exception {
    keytool(
        "-genkeypair",
        "-alias",
        alias,
        "-dname",
        "CN=" + alias,
        "-keyalg",
        "RSA",
        "-keysize",
        "2048",
        "-validity",
        "2",
        "-keystore",
        file(alias + ".p12"));
    keytool(
        "-certreq",
        "-alias",
        alias,
        "-keystore",
        file(alias + ".p12"),
        "-file",
        file(alias + ".csr"));
    keytool(
        "-gencert",
        "-alias",
        "ca",
        "-keystore",
        file("ca.p12"),
        "-infile",
        file(alias + ".csr"),
        "-outfile",
        file(alias + ".pem"),
        "-rfc",
        "-validity",
        "2",
        "-ext",
        "EKU=" + usage,
        "-ext",
        san);
    KeyStore store = KeyStore.getInstance("PKCS12");
    try (var input = Files.newInputStream(Path.of(file(alias + ".p12")))) {
      store.load(input, PASSWORD.toCharArray());
    }
    byte[] key = store.getKey(alias, PASSWORD.toCharArray()).getEncoded();
    Files.writeString(
        Path.of(file(alias + ".key")),
        "-----BEGIN PRIVATE KEY-----\n"
            + Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(key)
            + "\n-----END PRIVATE KEY-----\n",
        StandardCharsets.US_ASCII);
  }

  private void keytool(String... args) throws Exception {
    String executable =
        System.getProperty("os.name").startsWith("Windows") ? "keytool.exe" : "keytool";
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", executable).toString());
    command.addAll(List.of(args));
    command.addAll(List.of("-storepass", PASSWORD, "-noprompt"));
    Path log = directory.resolve("keytool.log");
    Process process =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()))
            .start();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new IllegalStateException("Test certificate generation timed out");
    }
    if (process.exitValue() != 0) {
      throw new IllegalStateException(
          "Test certificate generation failed: " + Files.readString(log));
    }
  }
}
