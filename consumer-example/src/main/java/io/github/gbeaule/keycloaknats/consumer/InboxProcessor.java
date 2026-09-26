package io.github.gbeaule.keycloaknats.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import javax.sql.DataSource;

/** The inbox insert and ALL database side effects must share this transaction. */
public final class InboxProcessor {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  /** A business effect that must use the supplied transaction and avoid external side effects. */
  @FunctionalInterface
  public interface Handler {
    /** Applies the event using the same transaction as its deduplication record. */
    void apply(Connection transaction, JsonNode event) throws Exception;
  }

  private final BoundedTransaction transactions;
  private final String consumer;

  /** Binds a logical consumer's deduplication namespace to its application database. */
  public InboxProcessor(DataSource database, String consumer) {
    this(database, consumer, ProcessingLimits.defaults());
  }

  /**
   * Uses independent bounded transactions, including when called concurrently by multiple workers.
   */
  public InboxProcessor(DataSource database, String consumer, ProcessingLimits limits) {
    if (consumer == null || !consumer.matches("[A-Za-z0-9_-]{1,128}")) {
      throw new IllegalArgumentException("Invalid consumer name");
    }
    this.transactions = new BoundedTransaction(database, limits);
    this.consumer = consumer;
  }

  /** Creates the example inbox and effect ledger; production applications should own migrations. */
  public static void initialize(DataSource database) throws SQLException {
    try (Connection db = database.getConnection();
        var statement = db.createStatement();
        var script = InboxProcessor.class.getResourceAsStream("/db/consumer.sql")) {
      if (script == null) {
        throw new SQLException("Missing consumer migration");
      }
      statement.setQueryTimeout(30);
      statement.execute(new String(script.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException failure) {
      throw new SQLException("Cannot read consumer migration");
    }
  }

  /**
   * Returns false for a previously committed event. The caller ACKs only after this method returns.
   */
  public boolean process(byte[] payload, Handler handler) throws Exception {
    if (payload == null || payload.length > 1048576) {
      throw new RejectedEventException(RejectedEventException.Reason.OVERSIZE);
    }
    JsonNode event;
    try {
      event = objectMapper.readTree(payload);
    } catch (IOException failure) {
      throw new RejectedEventException(RejectedEventException.Reason.INVALID_JSON);
    }
    if (event == null
        || !"1.0".equals(event.path("specversion").asText())
        || !"urn:keycloak-nats:event:v1".equals(event.path("dataschema").asText())
        || !event.path("source").asText().startsWith("urn:keycloak:realm:")
        || !event.path("type").asText().startsWith("io.keycloak.")
        || !event.path("data").isObject()) {
      throw new RejectedEventException(RejectedEventException.Reason.UNSUPPORTED_ENVELOPE);
    }
    UUID id;
    try {
      id = UUID.fromString(event.path("id").asText());
      if (!id.toString().equals(event.path("id").asText())) {
        throw new IllegalArgumentException();
      }
    } catch (IllegalArgumentException failure) {
      throw new RejectedEventException(RejectedEventException.Reason.INVALID_EVENT_ID);
    }
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(payload);
    return transactions.execute(
        db -> {
          boolean inserted;
          try (var insert =
              db.prepareStatement(
                  """
                INSERT INTO knd_inbox(consumer_name,event_id,payload_hash)
                VALUES (?,?,?) ON CONFLICT DO NOTHING
                """)) {
            insert.setString(1, consumer);
            insert.setObject(2, id);
            insert.setBytes(3, hash);
            inserted = insert.executeUpdate() == 1;
          }
          if (inserted) {
            handler.apply(db, event);
          } else {
            try (var existing =
                db.prepareStatement(
                    "SELECT payload_hash FROM knd_inbox WHERE consumer_name=? AND event_id=?")) {
              existing.setString(1, consumer);
              existing.setObject(2, id);
              try (var result = existing.executeQuery()) {
                if (!result.next() || !MessageDigest.isEqual(hash, result.getBytes(1))) {
                  throw new IllegalStateException("Event ID reused with different content");
                }
              }
            }
          }
          return inserted;
        });
  }

  /** Records one example effect; replace this with application work in the supplied transaction. */
  public void recordEffect(Connection db, JsonNode event) throws SQLException {
    try (var statement =
        db.prepareStatement(
            "INSERT INTO knd_effects(consumer_name,event_id,event_type) VALUES (?,?,?)")) {
      statement.setString(1, consumer);
      statement.setObject(2, UUID.fromString(event.path("id").asText()));
      statement.setString(3, event.path("type").asText());
      statement.executeUpdate();
    }
  }
}
