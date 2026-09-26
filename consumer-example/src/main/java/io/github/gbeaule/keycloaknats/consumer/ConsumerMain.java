package io.github.gbeaule.keycloaknats.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.gbeaule.keycloaknats.jetstream.StreamPolicy;
import io.nats.client.Connection;
import io.nats.client.JetStreamOptions;
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

/** Runs the bounded inbox consumer and explicit provisioning, migration and recovery commands. */
public final class ConsumerMain {
  private ConsumerMain() {}

  /** All credentials come from environment settings, never command arguments or diagnostic text. */
  public static void main(String[] args) {
    try {
      run(args);
    } catch (InterruptedException stopped) {
      Thread.currentThread().interrupt();
    } catch (Exception failure) {
      System.err.println(
          "Consumer stopped; failure category=" + failure.getClass().getSimpleName());
      System.exit(1);
    }
  }

  private static void run(String[] args) throws Exception {
    String command = args.length == 0 ? "run" : args[0];
    if (!java.util.Set.of("run", "provision", "migrate", "quarantine-list", "quarantine-replay")
            .contains(command)
        || ("quarantine-replay".equals(command)
            ? args.length != 3
            : "quarantine-list".equals(command) ? args.length > 2 : args.length > 1)) {
      System.err.println(
          "Usage: [run|provision|migrate|quarantine-list [limit]"
              + "|quarantine-replay stream sequence]");
      throw new IllegalArgumentException("Invalid command");
    }
    ConsumerConfig config = ConsumerConfig.system();
    ConsumerSettings settings = config.settings();
    try (var database =
        ConsumerDatabase.pool(
            config.database(settings.processing()), settings.processing(), config.poolSize())) {
      execute(args, command, settings, config, database);
    }
  }

  private static void execute(
      String[] args,
      String command,
      ConsumerSettings settings,
      ConsumerConfig config,
      javax.sql.DataSource database)
      throws Exception {
    final String stream = config.stream();
    final String subject = config.subjectPrefix() + ".>";
    final String durable = config.durable();
    if ("migrate".equals(command)) {
      InboxProcessor.initialize(database);
      System.out.println("Consumer schema migrated.");
      return;
    }
    var quarantine = new QuarantineStore(database, durable, settings.processing());
    if ("quarantine-list".equals(command)) {
      int limit = args.length == 2 ? Integer.parseInt(args[1]) : 100;
      System.out.println(new ObjectMapper().writeValueAsString(quarantine.pending(limit)));
      return;
    }
    try (Connection nats = connect(config.nats())) {
      JetStreamOptions requests =
          JetStreamOptions.builder()
              .requestTimeout(Duration.ofMillis(settings.ackTimeoutMs()))
              .build();
      if ("provision".equals(command)) {
        provision(nats, requests, stream, subject, durable, settings, config);
        return;
      }
      if ("quarantine-replay".equals(command)) {
        StreamPolicy.validate(
            nats.jetStreamManagement(requests).getStreamInfo(args[1]).getConfiguration(),
            args[1],
            config.subjectPrefix(),
            config.minReplicas(),
            0);
        boolean replayed =
            quarantine.replay(nats.jetStream(requests), args[1], Long.parseLong(args[2]));
        System.out.println(
            replayed
                ? "Replay published; original event bytes and ID preserved."
                : "Replay remains pending; retry after the stream deduplication window.");
        return;
      }
      var consumerConfig =
          nats.jetStreamManagement(requests)
              .getConsumerInfo(stream, durable)
              .getConsumerConfiguration();
      validateConsumer(consumerConfig);
      settings.validateProgress(consumerConfig);
      if (settings.autoMigrate()) {
        InboxProcessor.initialize(database);
      }
      checkSchema(database);
      InboxProcessor processor = new InboxProcessor(database, durable, settings.processing());
      var subscription =
          nats.jetStream(requests).subscribe(null, PullSubscribeOptions.bind(stream, durable));
      Thread main = Thread.currentThread();
      Thread shutdown = new Thread(main::interrupt, "knd-consumer-shutdown");
      Runtime.getRuntime().addShutdownHook(shutdown);
      try (var monitor =
              new ConsumerMonitor(settings, () -> nats.getStatus() == Connection.Status.CONNECTED);
          var worker = new ConsumerWorker(processor, quarantine, settings, monitor)) {
        System.out.println(
            "Consumer running; failure_action="
                + settings.failures().action()
                + " deadline_ms="
                + settings.processing().deadlineMs());
        while (!Thread.currentThread().isInterrupted()) {
          for (var message : subscription.fetch(1, Duration.ofSeconds(2))) {
            worker.handle(message, processor::recordEffect);
          }
        }
      } finally {
        subscription.unsubscribe();
        try {
          Runtime.getRuntime().removeShutdownHook(shutdown);
        } catch (IllegalStateException shuttingDown) {
          // The hook already interrupted this thread during JVM shutdown.
        }
      }
    }
  }

  private static void checkSchema(javax.sql.DataSource database) throws Exception {
    try (var db = database.getConnection();
        var query = db.createStatement()) {
      query.execute("SELECT consumer_name,event_id,payload_hash FROM knd_inbox LIMIT 0");
      query.execute("SELECT consumer_name,disposition,replayed_at FROM knd_quarantine LIMIT 0");
    }
  }

  private static Connection connect(ConsumerConfig.NatsSettings config) throws Exception {
    Options.Builder options =
        new Options.Builder()
            .servers(config.servers())
            .connectionTimeout(Duration.ofSeconds(5))
            .maxReconnects(-1)
            .reconnectBufferSize(0)
            .errorListener(
                new io.nats.client.ErrorListener() {
                  @Override
                  public void errorOccurred(Connection connection, String error) {
                    System.err.println("Consumer NATS server error; inspect server diagnostics");
                  }

                  @Override
                  public void exceptionOccurred(Connection connection, Exception exception) {
                    System.err.println("Consumer NATS connection exception");
                  }
                });
    if (config.tls().enabled()) {
      options.sslContext(config.tls().createContext());
      options.hostnameResolveMode(Options.HostnameResolveMode.HappyEyeballs);
    }
    if (config.credentialsFile() != null) {
      options.authHandler(Nats.credentials(config.credentialsFile()));
    }
    if (config.token() != null) {
      options.token(config.token().toCharArray());
    }
    return Nats.connect(options.build());
  }

  private static void provision(
      Connection nats,
      JetStreamOptions requests,
      String stream,
      String subject,
      String durable,
      ConsumerSettings settings,
      ConsumerConfig config)
      throws Exception {
    // Create-only. Changing live retention or consumer policies is an explicit operator operation.
    nats.jetStreamManagement(requests)
        .addStream(
            StreamConfiguration.builder()
                .name(stream)
                .subjects(subject)
                .storageType(StorageType.File)
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .discardPolicy(DiscardPolicy.New)
                .maxAge(Duration.ZERO)
                .maxBytes(config.streamMaxBytes())
                .replicas(config.minReplicas())
                .duplicateWindow(Duration.ofMinutes(2))
                .build());
    nats.jetStreamManagement(requests)
        .createConsumer(
            stream,
            ConsumerConfiguration.builder()
                .durable(durable)
                .filterSubject(subject)
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.All)
                .ackWait(Duration.ofMillis(settings.ackWaitMs()))
                .maxDeliver(-1)
                .maxAckPending(settings.maxAckPending())
                .build());
    System.out.println("Provisioned stream and durable consumer.");
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
