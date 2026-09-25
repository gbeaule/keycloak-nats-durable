package io.github.keycloaknats;

public interface EventPublisher extends AutoCloseable {
  /** Must return only after the expected stream acknowledges durable acceptance. */
  void publish(OutboxEvent event) throws Exception;

  @Override
  void close();
}
