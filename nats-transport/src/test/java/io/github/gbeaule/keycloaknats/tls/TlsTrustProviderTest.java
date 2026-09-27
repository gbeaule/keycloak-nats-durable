package io.github.gbeaule.keycloaknats.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.util.List;
import java.util.Map;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

/** Exercises the public JCA boundary without allowing provider changes to overlap other tests. */
@Isolated
class TlsTrustProviderTest {
  @Test
  void unsupportedTrustManagersFailClosedInsteadOfBypassingHostnameVerification() {
    String previous = Security.getProperty("ssl.TrustManagerFactory.algorithm");
    assertNotNull(previous);
    var provider = new UnsupportedTrustProvider();
    assertTrue(Security.addProvider(provider) > 0);
    try {
      Security.setProperty("ssl.TrustManagerFactory.algorithm", "KndTestUnsupported");
      var config = new TlsConfig(true, null, null, null);
      var failure = assertThrows(GeneralSecurityException.class, config::createContext);
      assertEquals("An extended X509 trust manager is required", failure.getMessage());
    } finally {
      Security.setProperty("ssl.TrustManagerFactory.algorithm", previous);
      Security.removeProvider(provider.getName());
    }
  }

  private static final class UnsupportedTrustProvider extends Provider {
    private static final long serialVersionUID = 1L;

    UnsupportedTrustProvider() {
      super("KndUnsupportedTrustTest", "1.0", "Test provider without peer verification support");
      putService(
          new Service(
              this,
              "TrustManagerFactory",
              "KndTestUnsupported",
              UnsupportedTrustFactory.class.getName(),
              List.of(),
              Map.of()) {
            @Override
            public Object newInstance(Object parameter) {
              return new UnsupportedTrustFactory();
            }
          });
    }
  }

  private static final class UnsupportedTrustFactory extends TrustManagerFactorySpi {
    @Override
    protected void engineInit(KeyStore store) {}

    @Override
    protected void engineInit(ManagerFactoryParameters parameters) {}

    @Override
    protected TrustManager[] engineGetTrustManagers() {
      return new TrustManager[] {new TrustManager() {}};
    }
  }
}
