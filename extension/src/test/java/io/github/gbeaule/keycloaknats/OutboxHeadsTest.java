package io.github.gbeaule.keycloaknats;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;
import java.util.List;
import org.junit.jupiter.api.Test;

class OutboxHeadsTest {
  @Test
  @SuppressWarnings("unchecked")
  void locksBeforeReadingAndExplicitlyWritesTheEarliestSurvivorOrClearsTheHead() {
    var em = mock(EntityManager.class);
    var counter = new CaptureCounter();
    var head = CaptureFixtures.row("next", "subject", "{}", 23);
    head.failed(101, "retry");
    var query = (TypedQuery<OutboxEvent>) mock(TypedQuery.class, RETURNS_SELF);
    var update = mock(Query.class, RETURNS_SELF);
    when(em.find(CaptureCounter.class, "key", LockModeType.PESSIMISTIC_WRITE)).thenReturn(counter);
    when(em.createQuery(anyString(), eq(OutboxEvent.class))).thenReturn(query);
    when(em.createQuery(anyString())).thenReturn(update);
    when(query.getResultList()).thenReturn(List.of(head), List.of());
    OutboxHeads.refresh(em, "key");
    var order = inOrder(em, query, update);
    order.verify(em).flush();
    order.verify(em).find(CaptureCounter.class, "key", LockModeType.PESSIMISTIC_WRITE);
    order
        .verify(em)
        .createQuery(
            "from NatsOutboxEvent where orderingKey = :key order by userSequence",
            OutboxEvent.class);
    order.verify(query).setParameter("key", "key");
    order.verify(query).setMaxResults(1);
    order.verify(query).getResultList();
    order
        .verify(em)
        .createQuery(
            "update NatsCaptureCounter set headEventId = :id, headNextAttemptAt = :due,"
                + " headCreatedAt = :created where orderingKey = :key");
    order.verify(update).setParameter("id", "next");
    order.verify(update).setParameter("due", 101L);
    order.verify(update).setParameter("created", 23L);
    order.verify(update).setParameter("key", "key");
    order.verify(update).executeUpdate();
    OutboxHeads.refresh(em, "key");
    order.verify(update).setParameter("id", (Object) null);
    order.verify(update).setParameter("due", (Object) null);
    order.verify(update).setParameter("created", (Object) null);
    order.verify(update).setParameter("key", "key");
    order.verify(update).executeUpdate();
  }

  @Test
  void independentChangesOnlyFlushAndMissingCountersFailClosed() {
    var em = mock(EntityManager.class);
    OutboxHeads.refresh(em, null);
    verify(em).flush();
    verifyNoMoreInteractions(em);
    assertThrows(NullPointerException.class, () -> OutboxHeads.refresh(em, "missing"));
  }
}
