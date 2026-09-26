package io.github.gbeaule.keycloaknats.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
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
  }

  @Test
  void tlsCanUseJvmRootsWithoutClientCredentials() throws Exception {
    var config = TlsConfig.from(new String[] {"tls://localhost:4222"}, key -> null);
    assertTrue(config.enabled());
    assertNotNull(config.createContext());
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
    SSLContext client = configuration(identity, key).createContext();
    try (var server =
            (SSLServerSocket)
                serverContext(identity).getServerSocketFactory().createServerSocket(0);
        var executor = Executors.newSingleThreadExecutor()) {
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
                  return socket.getSession().getPeerPrincipal();
                }
              });
      try (var socket =
          (SSLSocket) client.getSocketFactory().createSocket("localhost", server.getLocalPort())) {
        socket.setSoTimeout(5000);
        socket.startHandshake();
        socket.getOutputStream().write(7);
        socket.getOutputStream().flush();
        assertEquals(9, socket.getInputStream().read());
        assertEquals(
            identity.certificate().getSubjectX500Principal(),
            socket.getSession().getPeerPrincipal());
      }
      assertEquals(
          identity.certificate().getSubjectX500Principal(), accepted.get(5, TimeUnit.SECONDS));
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
    assertThrows(GeneralSecurityException.class, config::createContext);
  }

  @Test
  void oversizedKeysAreRejectedBeforeParsing() throws Exception {
    var config = configuration(rsa, " ".repeat(1048577));
    assertThrows(IOException.class, config::createContext);
  }

  private TlsConfig configuration(Identity identity, String key) throws Exception {
    Path certificate = directory.resolve("identity.pem");
    Path privateKey = directory.resolve("identity.key");
    Files.writeString(certificate, pem(identity.certificate()));
    Files.writeString(privateKey, key);
    return new TlsConfig(
        true, certificate.toString(), certificate.toString(), privateKey.toString());
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

  private static SSLContext serverContext(Identity identity) throws Exception {
    // Independent JDK construction verifies that the PEM-loaded client can actually authenticate.
    KeyStore store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    store.setKeyEntry(
        "identity",
        identity.keys().getPrivate(),
        new char[0],
        new Certificate[] {identity.certificate()});
    store.setCertificateEntry("trusted", identity.certificate());
    var keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keys.init(store, new char[0]);
    var trusts = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trusts.init(store);
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
