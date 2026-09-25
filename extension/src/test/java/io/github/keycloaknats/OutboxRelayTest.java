package io.github.keycloaknats;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxRelayTest {
  private EntityManager em;
  private EventPublisher publisher;
  private OutboxEvent row;
  private OutboxRelay relay;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setup() {
    em = mock(EntityManager.class);
    publisher = mock(EventPublisher.class);
    var query = mock(TypedQuery.class, RETURNS_SELF);
    when(em.createQuery(anyString(), eq(OutboxEvent.class))).thenReturn(query);
    row = new OutboxEvent("id", "subject", "{}", 0);
    when(query.getResultList()).thenReturn(List.of(row), List.of());
    relay = new OutboxRelay(work -> work.apply(em), publisher, BridgeConfig.from(Map.of()));
  }

  @Test
  void removesOnlyAfterSuccessfulAcknowledgement() throws Exception {
    relay.run();
    var order = inOrder(publisher, em);
    order.verify(publisher).publish(row);
    order.verify(em).remove(row);
  }

  @Test
  void lostPublishAckLeavesOriginalIdAndPayloadForRetry() throws Exception {
    doThrow(new IOException("ack lost")).when(publisher).publish(row);
    relay.run();
    verify(em, never()).remove(any());
    assertEquals("id", row.id());
    assertEquals("{}", row.payload());
    assertEquals(1, row.attempts());
    assertTrue(row.nextAttemptAt() > System.currentTimeMillis());
    assertEquals("IOException", row.lastError());
  }

  @Test
  void interruptedPublishRetainsRowAndInterruptFlag() throws Exception {
    doThrow(new InterruptedException()).when(publisher).publish(row);
    try {
      relay.run();
      assertTrue(Thread.currentThread().isInterrupted());
      verify(em, never()).remove(any());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void failedCommitDoesNotPermanentlyStopScheduling() {
    var transactions = mock(OutboxRelay.Transactions.class);
    when(transactions.run(any())).thenThrow(new IllegalStateException()).thenReturn(false);
    var worker = new OutboxRelay(transactions, publisher, BridgeConfig.from(Map.of()));
    assertDoesNotThrow(worker::run);
    assertDoesNotThrow(worker::run);
    verify(transactions, times(2)).run(any());
  }

  @Test
  void stopPreventsMoreWork() {
    relay.stop();
    relay.run();
    verifyNoInteractions(publisher);
  }
}
