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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.hibernate.Session;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.jdbc.ReturningWork;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CaptureRepositoryTest {
  @Test
  void managedConnectionBindsExactIdentitiesAndUsesMappedSchema() throws Exception {
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
    when(statement.executeQuery()).thenReturn(result);
    when(result.next()).thenReturn(true, false);
    when(result.getLong(1)).thenReturn(12L);
    when(session.doReturningWork(any()))
        .thenAnswer(call -> ((ReturningWork<?>) call.getArgument(0)).execute(connection));
    var ordering = CaptureRepository.next(em, "realm", "f:provider:用户");
    assertEquals(12, ordering.sequence());
    verify(statement).setString(1, ordering.key());
    verify(statement).setString(2, "realm");
    verify(statement).setString(3, "f:provider:用户");
    var sql = ArgumentCaptor.forClass(String.class);
    verify(connection).prepareStatement(sql.capture());
    assertTrue(sql.getValue().startsWith("INSERT INTO \"bridge-data\".KC_NATS_CAPTURE_COUNTER"));
    assertTrue(sql.getValue().endsWith("< " + Long.MAX_VALUE + " RETURNING LAST_SEQUENCE"));
    verify(statement).close();
    verify(result).close();
    assertThrows(IllegalStateException.class, () -> CaptureRepository.next(em, "realm", "user"));
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
