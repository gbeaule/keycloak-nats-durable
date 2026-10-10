package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class RelayDatabaseConfigTest {
  @ParameterizedTest
  @CsvSource({
    "0,1,1",
    "3606,1,1",
    "10,0,1",
    "100,60001,1",
    "1,1001,1",
    "10,1000,0",
    "10,1000,1001"
  })
  void rejectsUnboundedOrInconsistentDatabaseWaits(int transaction, int statement, int lock) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RelayDatabaseConfig(transaction, statement, lock));
  }

  @Test
  void includesTheSupportedEndpoints() {
    assertEquals(1, new RelayDatabaseConfig(1, 1, 1).transactionTimeoutSeconds());
    assertEquals(60000, new RelayDatabaseConfig(3605, 60000, 60000).lockTimeoutMillis());
  }

  @Test
  void limitsAreTransactionLocalAndSetBeforeDatabaseWork() {
    var em = mock(EntityManager.class);
    var query = mock(Query.class, RETURNS_SELF);
    String sql =
        "select set_config('statement_timeout', :statement, true),"
            + " set_config('lock_timeout', :lock, true)";
    when(em.createNativeQuery(eq(sql), eq(Object[].class))).thenReturn(query);
    RelayDatabase.limitStatements(em, new RelayDatabaseConfig(10, 5000, 1000));
    var order = inOrder(em, query);
    order.verify(em).createNativeQuery(sql, Object[].class);
    order.verify(query).setParameter("statement", "5000");
    order.verify(query).setParameter("lock", "1000");
    order.verify(query).getSingleResult();
  }
}
