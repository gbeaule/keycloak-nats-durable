package io.github.gbeaule.keycloaknats.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class TlsConfigTest {
  @TempDir Path directory;
  private static Identity rsa;
  private static Identity ec;

  private record Identity(KeyPair keys, X509Certificate certificate) {}

  private enum InvalidKey {
    EMPTY,
    MALFORMED,
    PUBLIC_ONLY,
    ENCRYPTED,
    MULTIPLE
  }

  @BeforeAll
  static void identities() throws Exception {
    rsa = identity("RSA");
    ec = identity("EC");
  }

  @Test
  void plaintextRequiresNoTlsConfiguration() {
    var config = TlsConfig.from(new String[] {"nats://localhost:4222"}, key -> null);
    assertFalse(config.enabled());
    assertThrows(IllegalStateException.class, config::createContext);
    assertEquals("TlsConfig[enabled=false, mutual=false]", config.toString());
  }

  @Test
  void tlsCanUseJvmRootsWithoutClientCredentials() throws Exception {
    var config = TlsConfig.from(new String[] {"tls://localhost:4222"}, key -> null);
    assertTrue(config.enabled());
    assertNotNull(config.createContext());
    assertEquals("TlsConfig[enabled=true, mutual=false]", config.toString());
  }

  @Test
  void mixedSeedsAreRejectedInBothOrdersBeforeReadingTlsFiles() {
    for (String[] servers :
        new String[][] {{"tls://first", "nats://second"}, {"nats://first", "tls://second"}}) {
      var failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  TlsConfig.from(
                      servers,
                      key -> {
                        throw new AssertionError("Mixed seed list must fail first");
                      }));
      assertEquals("Do not mix TLS and plaintext NATS servers", failure.getMessage());
    }
  }

  @Test
  void clientCredentialsMustBePairedAndTlsFilesCannotBeUsedWithPlaintext() {
    for (boolean enabled : new boolean[] {false, true}) {
      assertThrows(
          IllegalArgumentException.class, () -> new TlsConfig(enabled, null, "secret-cert", null));
      assertThrows(
          IllegalArgumentException.class, () -> new TlsConfig(enabled, null, null, "secret-key"));
    }
    assertThrows(
        IllegalArgumentException.class, () -> new TlsConfig(false, "secret-ca", null, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new TlsConfig(false, null, "secret-cert", "secret-key"));
  }

  @Test
  void filePathsAreNormalizedAndNeverPrinted() {
    var config =
        TlsConfig.from(
            new String[] {" tls://first ", "tls://second"},
            Map.of(
                    "tls-ca-file",
                    " ca.pem ",
                    "tls-cert-file",
                    " client.pem ",
                    "tls-key-file",
                    " secret.key ")
                ::get);
    assertTrue(config.enabled());
    assertEquals("ca.pem", config.caFile());
    assertEquals("client.pem", config.certificateFile());
    assertEquals("secret.key", config.keyFile());
    assertEquals("TlsConfig[enabled=true, mutual=true]", config.toString());
    var blank = TlsConfig.from(new String[] {"tls://first"}, key -> " \t");
    assertNull(blank.caFile());
    assertNull(blank.certificateFile());
    assertNull(blank.keyFile());
  }

  @ParameterizedTest
  @CsvSource({"RSA,PRIVATE KEY", "RSA,RSA PRIVATE KEY", "EC,PRIVATE KEY", "EC,EC PRIVATE KEY"})
  void pemKeysCompleteMutualTlsHandshake(String algorithm, String label) throws Exception {
    Identity identity = algorithm.equals("RSA") ? rsa : ec;
    String key =
        pem(
            label.equals("PRIVATE KEY")
                ? new PemObject(label, identity.keys().getPrivate().getEncoded())
                : identity.keys().getPrivate());
    assertTrue(key.contains("-----BEGIN " + label + "-----"));
    var providers = Security.getProviders();
    SSLContext client = configuration(rsa, identity, key).createContext();
    assertEquals(java.util.List.of(providers), java.util.List.of(Security.getProviders()));
    try (var executor = Executors.newSingleThreadExecutor();
        var server =
            (SSLServerSocket)
                serverContext(rsa, identity).getServerSocketFactory().createServerSocket(0)) {
      server.setSoTimeout(5000);
      server.setNeedClientAuth(true);
      var accepted =
          executor.submit(
              () -> {
                try (var socket = (SSLSocket) server.accept()) {
                  socket.setSoTimeout(5000);
                  socket.startHandshake();
                  assertEquals(7, socket.getInputStream().read());
                  socket.getOutputStream().write(9);
                  socket.getOutputStream().flush();
                  return socket.getSession().getPeerCertificates()[0];
                }
              });
      try (var socket =
          (SSLSocket) client.getSocketFactory().createSocket("localhost", server.getLocalPort())) {
        socket.setSoTimeout(5000);
        socket.startHandshake();
        socket.getOutputStream().write(7);
        socket.getOutputStream().flush();
        assertEquals(9, socket.getInputStream().read());
        assertEquals(rsa.certificate(), socket.getSession().getPeerCertificates()[0]);
      }
      assertEquals(identity.certificate(), accepted.get(5, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @EnumSource(InvalidKey.class)
  void invalidOrAmbiguousPrivateKeysAreRejected(InvalidKey input) throws Exception {
    String valid = pem(new PemObject("PRIVATE KEY", rsa.keys().getPrivate().getEncoded()));
    String key =
        switch (input) {
          case EMPTY -> "";
          case MALFORMED -> "-----BEGIN PRIVATE KEY-----\ninvalid!\n-----END PRIVATE KEY-----\n";
          case PUBLIC_ONLY -> pem(rsa.keys().getPublic());
          case ENCRYPTED -> encryptedKey();
          case MULTIPLE -> valid + valid;
        };
    var config = configuration(rsa, key);
    var failure = assertThrows(GeneralSecurityException.class, config::createContext);
    assertNull(failure.getCause(), "Parser errors must not expose key material through causes");
    String reason =
        switch (input) {
          case MULTIPLE -> "Client key file must contain exactly one private key";
          case MALFORMED -> "Cannot parse client PEM private key";
          default ->
              "Client key must be an unencrypted PKCS#8, RSA PKCS#1 or EC SEC1 PEM private key";
        };
    assertEquals(reason, failure.getMessage());
  }

  @Test
  void oversizedKeysAreRejectedBeforeParsing() throws Exception {
    var config = configuration(rsa, " ".repeat(1048577));
    var failure = assertThrows(IOException.class, config::createContext);
    assertEquals("TLS file exceeds 1 MiB", failure.getMessage());
  }

  @Test
  void keyAtTheFileSizeLimitStillParsesAndLoads() throws Exception {
    String key = pem(new PemObject("PRIVATE KEY", rsa.keys().getPrivate().getEncoded()));
    assertNotNull(configuration(rsa, key + " ".repeat(1048576 - key.length())).createContext());
  }

  @Test
  void emptyCertificateFilesFailClosed() throws Exception {
    Path empty = directory.resolve("empty.pem");
    Files.writeString(empty, "");
    var config = new TlsConfig(true, empty.toString(), null, null);
    var failure = assertThrows(GeneralSecurityException.class, config::createContext);
    assertEquals("Certificate file is empty", failure.getMessage());
  }

  @Test
  void missingCertificateFilesDoNotFallBackToSystemRoots() {
    var config = new TlsConfig(true, directory.resolve("missing.pem").toString(), null, null);
    assertThrows(IOException.class, config::createContext);
  }

  @Test
  void privateCaBundleTrustsEveryConfiguredCertificate() throws Exception {
    Path bundle = directory.resolve("ca.pem");
    Files.writeString(bundle, pem(rsa.certificate()) + pem(ec.certificate()));
    SSLContext client = new TlsConfig(true, bundle.toString(), null, null).createContext();
    for (Identity serverIdentity : new Identity[] {rsa, ec}) {
      try (var executor = Executors.newSingleThreadExecutor();
          var server =
              (SSLServerSocket)
                  serverContext(serverIdentity, rsa)
                      .getServerSocketFactory()
                      .createServerSocket(0)) {
        server.setSoTimeout(5000);
        var received =
            executor.submit(
                () -> {
                  try (var socket = (SSLSocket) server.accept()) {
                    socket.setSoTimeout(5000);
                    socket.startHandshake();
                    int request = socket.getInputStream().read();
                    socket.getOutputStream().write(9);
                    socket.getOutputStream().flush();
                    return request;
                  }
                });
        try (var socket =
            (SSLSocket)
                client.getSocketFactory().createSocket("localhost", server.getLocalPort())) {
          socket.setSoTimeout(5000);
          socket.startHandshake();
          assertEquals(serverIdentity.certificate(), socket.getSession().getPeerCertificates()[0]);
          socket.getOutputStream().write(7);
          socket.getOutputStream().flush();
          assertEquals(9, socket.getInputStream().read());
        }
        assertEquals(7, received.get(5, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void trustedCertificateForAnotherHostnameIsRejectedBeforeApplicationData() throws Exception {
    SSLContext client = configuration(rsa, pem(rsa.keys().getPrivate())).createContext();
    assertHandshakeRejected(client, "127.0.0.1");
  }

  @Test
  void anUntrustedServerCertificateIsRejectedBeforeApplicationData() throws Exception {
    SSLContext client = configuration(ec, pem(ec.keys().getPrivate())).createContext();
    assertHandshakeRejected(client, "localhost");
  }

  private void assertHandshakeRejected(SSLContext client, String hostname) throws Exception {
    try (var executor = Executors.newSingleThreadExecutor();
        var server =
            (SSLServerSocket)
                serverContext(rsa, rsa).getServerSocketFactory().createServerSocket(0)) {
      server.setSoTimeout(5000);
      var received =
          executor.submit(
              () -> {
                try (var socket = (SSLSocket) server.accept()) {
                  socket.setSoTimeout(5000);
                  socket.startHandshake();
                  return socket.getInputStream().read();
                } catch (SSLException | SocketException rejected) {
                  // A rejected client handshake can close the transport before the server alert.
                  return -1;
                }
              });
      try (var socket =
          (SSLSocket) client.getSocketFactory().createSocket(hostname, server.getLocalPort())) {
        socket.setSoTimeout(5000);
        assertThrows(SSLException.class, socket::startHandshake);
      }
      assertEquals(
          -1,
          received.get(5, TimeUnit.SECONDS),
          "Untrusted peers must receive no application byte");
    }
  }

  private TlsConfig configuration(Identity identity, String key) throws Exception {
    return configuration(identity, identity, key);
  }

  private TlsConfig configuration(Identity trust, Identity identity, String key) throws Exception {
    Path roots = directory.resolve("roots.pem");
    Path certificate = directory.resolve("identity.pem");
    Path privateKey = directory.resolve("identity.key");
    Files.writeString(certificate, pem(identity.certificate()));
    Files.writeString(privateKey, key);
    Files.writeString(roots, pem(trust.certificate()));
    return new TlsConfig(true, roots.toString(), certificate.toString(), privateKey.toString());
  }

  private static Identity identity(String algorithm) throws Exception {
    // BC's EC export includes curve parameters needed by a standalone SEC1 key.
    var generator =
        algorithm.equals("EC")
            ? KeyPairGenerator.getInstance(algorithm, new BouncyCastleProvider())
            : KeyPairGenerator.getInstance(algorithm);
    if (algorithm.equals("RSA")) {
      generator.initialize(2048);
    } else {
      generator.initialize(new ECGenParameterSpec("secp256r1"));
    }
    KeyPair keys = generator.generateKeyPair();
    X500Name name = new X500Name("CN=localhost");
    var certificate =
        new JcaX509v3CertificateBuilder(
                name,
                BigInteger.ONE,
                Date.from(Instant.now().minusSeconds(60)),
                Date.from(Instant.now().plusSeconds(3600)),
                name,
                keys.getPublic())
            .addExtension(
                Extension.subjectAlternativeName,
                false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, "localhost")))
            .addExtension(
                Extension.extendedKeyUsage,
                false,
                new ExtendedKeyUsage(
                    new KeyPurposeId[] {
                      KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth
                    }))
            .build(
                new JcaContentSignerBuilder(
                        algorithm.equals("RSA") ? "SHA256withRSA" : "SHA256withECDSA")
                    .build(keys.getPrivate()));
    return new Identity(keys, new JcaX509CertificateConverter().getCertificate(certificate));
  }

  private static SSLContext serverContext(Identity identity, Identity trustedClient)
      throws Exception {
    // Independent JDK construction verifies that the PEM-loaded client can actually authenticate.
    KeyStore store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    store.setKeyEntry(
        "identity",
        identity.keys().getPrivate(),
        new char[0],
        new Certificate[] {identity.certificate()});
    KeyStore trusted = KeyStore.getInstance("PKCS12");
    trusted.load(null, null);
    trusted.setCertificateEntry("trusted", trustedClient.certificate());
    var keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keys.init(store, new char[0]);
    var trusts = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trusts.init(trusted);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keys.getKeyManagers(), trusts.getTrustManagers(), null);
    return context;
  }

  private static String encryptedKey() throws Exception {
    String algorithm = "PBEWithSHA1AndDESede";
    var key =
        SecretKeyFactory.getInstance(algorithm)
            .generateSecret(new PBEKeySpec("test-only".toCharArray()));
    Cipher cipher = Cipher.getInstance(algorithm);
    cipher.init(Cipher.ENCRYPT_MODE, key, new PBEParameterSpec(new byte[8], 1000));
    var encrypted =
        new EncryptedPrivateKeyInfo(
            cipher.getParameters(), cipher.doFinal(rsa.keys().getPrivate().getEncoded()));
    return pem(new PemObject("ENCRYPTED PRIVATE KEY", encrypted.getEncoded()));
  }

  private static String pem(Object value) throws IOException {
    var text = new StringWriter();
    try (var writer = new JcaPEMWriter(text)) {
      writer.writeObject(value);
    }
    return text.toString();
  }
}
