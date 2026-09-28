package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.hibernate.Session;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.jdbc.ReturningWork;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

class CaptureRepositoryTest {
  @ParameterizedTest
  @CsvSource({"1,12", "2,1", "3,12", "0,0"})
  void managedConnectionAllocatesExistingNewAndRacingCountersOrRejectsOverflow(
      int successAt, long sequence) throws Exception {
    var em = mock(EntityManager.class);
    var session = mock(Session.class);
    var factory = mock(SessionFactoryImplementor.class);
    var metamodel = mock(MappingMetamodelImplementor.class);
    var mapping = mock(AbstractEntityPersister.class);
    var connection = mock(Connection.class);
    var statement = mock(PreparedStatement.class);
    var result = mock(ResultSet.class);
    when(em.unwrap(Session.class)).thenReturn(session);
    when(session.getSessionFactory()).thenReturn(factory);
    when(factory.unwrap(SessionFactoryImplementor.class)).thenReturn(factory);
    when(factory.getMappingMetamodel()).thenReturn(metamodel);
    when(metamodel.getEntityDescriptor(CaptureCounter.class)).thenReturn(mapping);
    when(mapping.getTableName()).thenReturn("\"bridge-data\".KC_NATS_CAPTURE_COUNTER");
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    var executions = new AtomicInteger();
    when(statement.executeQuery())
        .thenAnswer(
            call -> {
              executions.incrementAndGet();
              return result;
            });
    when(result.next()).thenAnswer(call -> executions.get() == successAt);
    when(result.getLong(1)).thenReturn(sequence);
    when(session.doReturningWork(any()))
        .thenAnswer(call -> ((ReturningWork<?>) call.getArgument(0)).execute(connection));
    var identity = new EventOrdering("realm", "f:provider:用户", 1);
    if (successAt == 0) {
      assertThrows(
          IllegalStateException.class,
          () -> CaptureRepository.next(em, identity.realmId(), identity.userId()));
    } else {
      var ordering = CaptureRepository.next(em, identity.realmId(), identity.userId());
      assertEquals(sequence, ordering.sequence());
      assertEquals(identity.key(), ordering.key());
    }
    int statements = successAt == 0 ? 3 : successAt;
    verify(statement, times(statements)).setString(1, identity.key());
    verify(statement, times(statements)).setString(2, identity.realmId());
    verify(statement, times(statements)).setString(3, identity.userId());
    var sql = ArgumentCaptor.forClass(String.class);
    verify(connection, times(statements)).prepareStatement(sql.capture());
    var queries = sql.getAllValues();
    assertTrue(queries.getFirst().startsWith("UPDATE \"bridge-data\".KC_NATS_CAPTURE_COUNTER"));
    assertTrue(queries.getFirst().endsWith("< " + Long.MAX_VALUE + " RETURNING LAST_SEQUENCE"));
    if (statements > 1) {
      assertTrue(queries.get(1).startsWith("INSERT INTO \"bridge-data\".KC_NATS_CAPTURE_COUNTER"));
      assertTrue(queries.get(1).endsWith("ON CONFLICT DO NOTHING RETURNING LAST_SEQUENCE"));
    }
    if (statements == 3) {
      assertEquals(queries.getFirst(), queries.getLast());
    }
    verify(statement, times(statements)).close();
    verify(result, times(statements)).close();
    verify(connection, never()).commit();
    verify(connection, never()).close();
    new CaptureCounter();
  }

  @Test
  void independentCaptureReadsDatabaseTimeWithoutAccessingCounters() {
    var em = mock(EntityManager.class);
    assertNull(CaptureRepository.next(em, "realm", null));
    verifyNoInteractions(em);
    var query = mock(Query.class);
    when(em.createNativeQuery(anyString(), eq(Long.class))).thenReturn(query);
    when(query.getSingleResult()).thenReturn(123L);
    assertEquals(123, CaptureRepository.databaseTime(em));
  }
}
