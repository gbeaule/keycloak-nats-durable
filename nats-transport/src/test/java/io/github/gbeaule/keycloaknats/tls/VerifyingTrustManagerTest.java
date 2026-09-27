package io.github.gbeaule.keycloaknats.tls;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;
import org.junit.jupiter.api.Test;

class VerifyingTrustManagerTest {
  private final RecordingTrustManager delegate = new RecordingTrustManager();
  private final VerifyingTrustManager manager = new VerifyingTrustManager(delegate);
  private final X509Certificate[] chain = new X509Certificate[0];

  @Test
  void socketVerificationEnablesHostnameChecksBeforeDelegatingAndPreservesOtherParameters()
      throws Exception {
    try (var socket = (SSLSocket) SSLContext.getDefault().getSocketFactory().createSocket()) {
      final String[] protocols = socket.getEnabledProtocols();
      final String[] ciphers = socket.getEnabledCipherSuites();
      manager.checkServerTrusted(chain, "RSA", socket);
      assertEquals("HTTPS", socket.getSSLParameters().getEndpointIdentificationAlgorithm());
      assertArrayEquals(protocols, socket.getEnabledProtocols());
      assertArrayEquals(ciphers, socket.getEnabledCipherSuites());
      assertDelegated("server-socket", socket);
    }
  }

  @Test
  void engineVerificationEnablesHostnameChecksBeforeDelegatingAndPreservesThePeer()
      throws Exception {
    var engine = SSLContext.getDefault().createSSLEngine("broker.example", 4222);
    final String[] protocols = engine.getEnabledProtocols();
    manager.checkServerTrusted(chain, "RSA", engine);
    assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    assertEquals("broker.example", engine.getPeerHost());
    assertEquals(4222, engine.getPeerPort());
    assertArrayEquals(protocols, engine.getEnabledProtocols());
    assertDelegated("server-engine", engine);
  }

  @Test
  void serverVerificationWithoutTlsPeerContextFailsBeforeConsultingTheDelegate() throws Exception {
    try (var socket = new Socket()) {
      assertThrows(
          CertificateException.class, () -> manager.checkServerTrusted(chain, "RSA", socket));
      assertThrows(
          CertificateException.class,
          () -> manager.checkServerTrusted(chain, "RSA", (Socket) null));
      assertThrows(CertificateException.class, () -> manager.checkServerTrusted(chain, "RSA"));
      assertEquals(List.of(), delegate.calls);
    }
  }

  @Test
  void serverCertificateRejectionsAreNeverSwallowed() throws Exception {
    delegate.failure = new CertificateException("untrusted issuer");
    try (var socket = (SSLSocket) SSLContext.getDefault().getSocketFactory().createSocket()) {
      assertSame(
          delegate.failure,
          assertThrows(
              CertificateException.class, () -> manager.checkServerTrusted(chain, "RSA", socket)));
    }
    var engine = SSLContext.getDefault().createSSLEngine("broker.example", 4222);
    assertSame(
        delegate.failure,
        assertThrows(
            CertificateException.class, () -> manager.checkServerTrusted(chain, "RSA", engine)));
    assertEquals(List.of("server-socket", "server-engine"), delegate.calls);
  }

  @Test
  void clientVerificationPreservesTheSelectedOverloadAndArguments() throws Exception {
    try (var socket = (SSLSocket) SSLContext.getDefault().getSocketFactory().createSocket()) {
      String algorithm = socket.getSSLParameters().getEndpointIdentificationAlgorithm();
      manager.checkClientTrusted(chain, "RSA", socket);
      assertDelegated("client-socket", socket);
      assertEquals(algorithm, socket.getSSLParameters().getEndpointIdentificationAlgorithm());
    }
    delegate.calls.clear();
    var engine = SSLContext.getDefault().createSSLEngine();
    String algorithm = engine.getSSLParameters().getEndpointIdentificationAlgorithm();
    manager.checkClientTrusted(chain, "RSA", engine);
    assertDelegated("client-engine", engine);
    assertEquals(algorithm, engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    delegate.calls.clear();
    manager.checkClientTrusted(chain, "RSA");
    assertDelegated("client", null);
    assertSame(delegate.issuers, manager.getAcceptedIssuers());
  }

  @Test
  void clientCertificateRejectionsAreNeverSwallowed() throws Exception {
    delegate.failure = new CertificateException("untrusted client");
    try (var socket = new Socket()) {
      assertSame(
          delegate.failure,
          assertThrows(
              CertificateException.class, () -> manager.checkClientTrusted(chain, "RSA", socket)));
    }
    var engine = SSLContext.getDefault().createSSLEngine();
    assertSame(
        delegate.failure,
        assertThrows(
            CertificateException.class, () -> manager.checkClientTrusted(chain, "RSA", engine)));
    assertSame(
        delegate.failure,
        assertThrows(CertificateException.class, () -> manager.checkClientTrusted(chain, "RSA")));
    assertEquals(List.of("client-socket", "client-engine", "client"), delegate.calls);
  }

  private void assertDelegated(String method, Object context) {
    assertEquals(List.of(method), delegate.calls);
    assertSame(chain, delegate.chain);
    assertEquals("RSA", delegate.authType);
    assertSame(context, delegate.context);
  }

  private static final class RecordingTrustManager extends X509ExtendedTrustManager {
    private final List<String> calls = new ArrayList<>();
    private final X509Certificate[] issuers = new X509Certificate[0];
    private X509Certificate[] chain;
    private String authType;
    private Object context;
    private CertificateException failure;

    private void record(
        String method, X509Certificate[] certificates, String authentication, Object peer)
        throws CertificateException {
      calls.add(method);
      chain = certificates;
      authType = authentication;
      context = peer;
      if (failure != null) {
        throw failure;
      }
    }

    @Override
    public void checkServerTrusted(
        X509Certificate[] certificates, String authentication, Socket socket)
        throws CertificateException {
      assertEquals(
          "HTTPS", ((SSLSocket) socket).getSSLParameters().getEndpointIdentificationAlgorithm());
      record("server-socket", certificates, authentication, socket);
    }

    @Override
    public void checkServerTrusted(
        X509Certificate[] certificates, String authentication, SSLEngine engine)
        throws CertificateException {
      assertEquals("HTTPS", engine.getSSLParameters().getEndpointIdentificationAlgorithm());
      record("server-engine", certificates, authentication, engine);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certificates, String authentication)
        throws CertificateException {
      record("server", certificates, authentication, null);
    }

    @Override
    public void checkClientTrusted(
        X509Certificate[] certificates, String authentication, Socket socket)
        throws CertificateException {
      record("client-socket", certificates, authentication, socket);
    }

    @Override
    public void checkClientTrusted(
        X509Certificate[] certificates, String authentication, SSLEngine engine)
        throws CertificateException {
      record("client-engine", certificates, authentication, engine);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] certificates, String authentication)
        throws CertificateException {
      record("client", certificates, authentication, null);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return issuers;
    }
  }
}
