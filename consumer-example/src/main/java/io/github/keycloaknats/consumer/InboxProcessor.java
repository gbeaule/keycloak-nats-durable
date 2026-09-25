package io.github.keycloaknats.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

  private final DataSource database;
  private final String consumer;

  /** Binds a logical consumer's deduplication namespace to its application database. */
  public InboxProcessor(DataSource database, String consumer) {
    if (consumer == null || !consumer.matches("[A-Za-z0-9_-]{1,128}")) {
      throw new IllegalArgumentException("Invalid consumer name");
    }
    this.database = database;
    this.consumer = consumer;
  }

  /** Creates the example inbox and effect ledger; production applications should own migrations. */
  public static void initialize(DataSource database) throws SQLException {
    try (Connection db = database.getConnection();
        var statement = db.createStatement()) {
      statement.execute(
          """
          CREATE TABLE IF NOT EXISTS knd_inbox (
            consumer_name VARCHAR(128) NOT NULL,
            event_id UUID NOT NULL,
            payload_hash BYTEA NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(consumer_name, event_id)
          )
          """);
      statement.execute(
          """
          CREATE TABLE IF NOT EXISTS knd_effects (
            consumer_name VARCHAR(128) NOT NULL,
            event_id UUID NOT NULL,
            event_type TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
          )
          """);
    }
  }

  /**
   * Returns false for a previously committed event. The caller ACKs only after this method returns.
   */
  public boolean process(byte[] payload, Handler handler) throws Exception {
    if (payload.length > 1048576) {
      throw new IllegalArgumentException("Event is too large");
    }
    JsonNode event = objectMapper.readTree(payload);
    if (event == null
        || !"1.0".equals(event.path("specversion").asText())
        || !"urn:keycloak-nats:event:v1".equals(event.path("dataschema").asText())
        || !event.path("source").asText().startsWith("urn:keycloak:realm:")
        || !event.path("type").asText().startsWith("io.keycloak.")
        || !event.path("data").isObject()) {
      throw new IllegalArgumentException("Unsupported event envelope");
    }
    UUID id = UUID.fromString(event.path("id").asText());
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(payload);
    try (Connection db = database.getConnection()) {
      db.setAutoCommit(false);
      try {
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
        db.commit();
        return inserted;
      } catch (Exception | Error failure) {
        try {
          db.rollback();
        } catch (SQLException rollback) {
          failure.addSuppressed(rollback);
        }
        throw failure;
      }
    }
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
