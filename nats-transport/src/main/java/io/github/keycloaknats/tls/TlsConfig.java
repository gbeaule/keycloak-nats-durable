package io.github.keycloaknats.tls;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.function.Function;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

/**
 * Shared PEM configuration for publisher, provisioner and consumer; never disables verification.
 */
public record TlsConfig(boolean enabled, String caFile, String certificateFile, String keyFile) {
  /** Validates paired client credentials; certificate settings apply only when TLS is enabled. */
  public TlsConfig {
    if ((certificateFile == null) != (keyFile == null)) {
      throw new IllegalArgumentException(
          "tls-cert-file and tls-key-file must be supplied together");
    }
    if (!enabled && (caFile != null || certificateFile != null)) {
      throw new IllegalArgumentException("TLS files require tls:// on every NATS server");
    }
  }

  /** Reads optional file paths; mixed plaintext/TLS seed lists are rejected. */
  public static TlsConfig from(String[] servers, Function<String, String> get) {
    boolean anyTls = false;
    boolean allTls = true;
    for (String server : servers) {
      boolean tls = server.trim().startsWith("tls://");
      anyTls |= tls;
      allTls &= tls;
    }
    if (anyTls && !allTls) {
      throw new IllegalArgumentException("Do not mix TLS and plaintext NATS servers");
    }
    return new TlsConfig(
        anyTls, path(get, "tls-ca-file"), path(get, "tls-cert-file"), path(get, "tls-key-file"));
  }

  /** Loads a private CA bundle or system roots, with an optional PEM client key and chain. */
  public SSLContext createContext() throws GeneralSecurityException, IOException {
    if (!enabled) {
      throw new IllegalStateException("TLS is not enabled");
    }
    KeyStore roots = null;
    if (caFile != null) {
      roots = emptyStore();
      Certificate[] certificates = certificates(caFile);
      for (int i = 0; i < certificates.length; i++) {
        roots.setCertificateEntry("ca-" + i, certificates[i]);
      }
    }
    TrustManagerFactory trusts =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trusts.init(roots);
    TrustManager[] managers = trusts.getTrustManagers();
    for (int i = 0; i < managers.length; i++) {
      if (!(managers[i] instanceof X509ExtendedTrustManager extended)) {
        throw new GeneralSecurityException("An extended X509 trust manager is required");
      }
      managers[i] = new VerifyingTrustManager(extended);
    }
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(clientKeys(), managers, null);
    return context;
  }

  private KeyManager[] clientKeys() throws GeneralSecurityException, IOException {
    if (certificateFile == null) {
      return null;
    }
    Certificate[] chain = certificates(certificateFile);
    PrivateKey key = privateKey(read(keyFile));
    KeyStore keys = emptyStore();
    char[] password = new char[0];
    keys.setKeyEntry("client", key, password, chain);
    KeyManagerFactory factory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    factory.init(keys, password);
    return factory.getKeyManagers();
  }

  private static PrivateKey privateKey(byte[] pem) throws GeneralSecurityException {
    try (var parser =
        new PEMParser(
            new InputStreamReader(new ByteArrayInputStream(pem), StandardCharsets.US_ASCII))) {
      Object parsed = parser.readObject();
      if (parser.readObject() != null) {
        throw new GeneralSecurityException("Client key file must contain exactly one private key");
      }
      PrivateKeyInfo info;
      if (parsed instanceof PrivateKeyInfo pkcs8) {
        info = pkcs8;
      } else if (parsed instanceof PEMKeyPair pair) {
        info = pair.getPrivateKeyInfo();
      } else {
        throw new GeneralSecurityException(
            "Client key must be an unencrypted PKCS#8, RSA PKCS#1 or EC SEC1 PEM private key");
      }
      // The converter uses JCA providers already present in the JVM; no global provider changes.
      return new JcaPEMKeyConverter().getPrivateKey(info);
    } catch (IOException | RuntimeException invalid) {
      // Parser messages can contain input. Keep key material out of caller diagnostics.
      throw new GeneralSecurityException("Cannot parse client PEM private key");
    }
  }

  private static KeyStore emptyStore() throws GeneralSecurityException, IOException {
    KeyStore store = KeyStore.getInstance("PKCS12");
    store.load(null, null);
    return store;
  }

  private static Certificate[] certificates(String file)
      throws GeneralSecurityException, IOException {
    var certificates =
        CertificateFactory.getInstance("X.509")
            .generateCertificates(new ByteArrayInputStream(read(file)));
    if (certificates.isEmpty()) {
      throw new GeneralSecurityException("Certificate file is empty");
    }
    return certificates.toArray(Certificate[]::new);
  }

  private static byte[] read(String file) throws IOException {
    try (var input = Files.newInputStream(Path.of(file))) {
      byte[] bytes = input.readNBytes(1048577);
      if (bytes.length > 1048576) {
        throw new IOException("TLS file exceeds 1 MiB");
      }
      return bytes;
    }
  }

  private static String path(Function<String, String> get, String key) {
    String value = get.apply(key);
    return value == null || value.isBlank() ? null : value.trim();
  }

  @Override
  public String toString() {
    return "TlsConfig[enabled=" + enabled + ", mutual=" + (certificateFile != null) + "]";
  }
}
