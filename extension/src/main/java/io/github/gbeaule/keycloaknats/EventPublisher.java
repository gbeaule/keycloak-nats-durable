package io.github.gbeaule.keycloaknats;

/** Publishes persisted event bytes and returns only after a successful broker acknowledgement. */
public interface EventPublisher extends AutoCloseable {
  /** Must return only after the expected stream acknowledges durable acceptance. */
  void publish(OutboxEvent event) throws Exception;

  @Override
  void close();
}
