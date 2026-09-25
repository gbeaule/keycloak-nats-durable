package io.github.keycloaknats.consumer;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.time.Duration;
import org.postgresql.ds.PGSimpleDataSource;

/** Provisions durable delivery or runs the example consumer with a transactional database inbox. */
public final class ConsumerMain {
  private ConsumerMain() {}

  /** Reads deployment settings from the environment; {@code provision} creates broker resources. */
  public static void main(String[] args) throws Exception {
    String stream = env("KND_STREAM", "KEYCLOAK_EVENTS");
    String subject = env("KND_SUBJECT_PREFIX", "keycloak.events") + ".>";
    String durable = env("KND_CONSUMER", "auth-worker");
    Options.Builder options =
        new Options.Builder()
            .servers(env("KND_NATS_URL", "nats://localhost:4222").split(","))
            .connectionTimeout(Duration.ofSeconds(5))
            .maxReconnects(-1);
    if (System.getenv("KND_CREDENTIALS_FILE") != null) {
      options.authHandler(Nats.credentials(System.getenv("KND_CREDENTIALS_FILE")));
    }
    if (System.getenv("KND_TOKEN") != null) {
      options.token(System.getenv("KND_TOKEN").toCharArray());
    }
    try (Connection nats = Nats.connect(options.build())) {
      if (args.length == 1 && "provision".equals(args[0])) {
        // Deliberately create-only. Changing live retention/consumer policies requires operator
        // review.
        nats.jetStreamManagement()
            .addStream(
                StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.File)
                    .retentionPolicy(RetentionPolicy.WorkQueue)
                    .discardPolicy(DiscardPolicy.New)
                    .maxAge(Duration.ZERO)
                    .maxBytes(Long.parseLong(env("KND_STREAM_MAX_BYTES", "1073741824")))
                    .replicas(Integer.parseInt(env("KND_MIN_REPLICAS", "3")))
                    .duplicateWindow(Duration.ofMinutes(2))
                    .build());
        nats.jetStreamManagement()
            .createConsumer(
                stream,
                ConsumerConfiguration.builder()
                    .durable(durable)
                    .filterSubject(subject)
                    .ackPolicy(AckPolicy.Explicit)
                    .deliverPolicy(DeliverPolicy.All)
                    .ackWait(Duration.ofSeconds(30))
                    .maxDeliver(-1)
                    .maxAckPending(1000)
                    .build());
        System.out.println("Provisioned stream and durable consumer.");
        return;
      }
      var consumerConfig =
          nats.jetStreamManagement().getConsumerInfo(stream, durable).getConsumerConfiguration();
      validateConsumer(consumerConfig);
      PGSimpleDataSource database = new PGSimpleDataSource();
      database.setURL(env("KND_CONSUMER_DB_URL", "jdbc:postgresql://localhost:5432/consumer"));
      database.setUser(env("KND_CONSUMER_DB_USER", "consumer"));
      database.setPassword(env("KND_CONSUMER_DB_PASSWORD", ""));
      InboxProcessor.initialize(database);
      InboxProcessor processor = new InboxProcessor(database, durable);
      var subscription =
          nats.jetStream().subscribe(null, PullSubscribeOptions.bind(stream, durable));
      while (!Thread.currentThread().isInterrupted()) {
        for (var message : subscription.fetch(1, Duration.ofSeconds(2))) {
          try {
            processor.process(message.getData(), processor::recordEffect);
            message.ackSync(Duration.ofSeconds(5));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (Exception e) {
            // No ACK on failure. NATS redelivers after AckWait, including a crash after DB commit.
            System.err.println("Event pending; failure category=" + e.getClass().getSimpleName());
          }
        }
      }
    }
  }

  private static String env(String name, String fallback) {
    return System.getenv().getOrDefault(name, fallback);
  }

  static void validateConsumer(ConsumerConfiguration consumer) {
    if (consumer.getDurable() == null
        || consumer.getDurable().isBlank()
        || consumer.getAckPolicy() != AckPolicy.Explicit
        || consumer.getMaxDeliver() != -1
        || consumer.isMemStorage()
        || consumer.isHeadersOnly()
        || consumer.getDeliverPolicy() != DeliverPolicy.All
        || (consumer.getInactiveThreshold() != null && !consumer.getInactiveThreshold().isZero())
        || consumer.getDeliverSubject() != null) {
      throw new IllegalStateException(
          "Require a durable pull consumer with explicit ACKs and disk state;"
              + " retain full history and payloads with no expiry and unlimited redelivery");
    }
  }
}
