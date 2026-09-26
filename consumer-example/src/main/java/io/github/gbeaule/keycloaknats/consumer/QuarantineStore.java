package io.github.gbeaule.keycloaknats.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.impl.Headers;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;

/** Durable recovery copies and discard audit metadata; never deletes broker messages itself. */
public final class QuarantineStore {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final BoundedTransaction transactions;
  private final String consumer;

  /** Uses the same application database and migration as the inbox. */
  public QuarantineStore(DataSource database, String consumer, ProcessingLimits limits) {
    if (consumer == null || !consumer.matches("[A-Za-z0-9_-]{1,128}")) {
      throw new IllegalArgumentException("Invalid consumer name");
    }
    this.consumer = consumer;
    transactions = new BoundedTransaction(database, limits);
  }

  /**
   * Commits the complete original event for quarantine, or only audit metadata for explicit loss.
   */
  public void record(Message message, FailurePolicy.Action action, String reason) throws Exception {
    if (action == FailurePolicy.Action.RETRY || !reason.matches("[A-Z_]{1,64}")) {
      throw new IllegalArgumentException("Invalid recovery action or reason code");
    }
    var metadata = message.metaData();
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(message.getData());
    boolean retain = action == FailurePolicy.Action.QUARANTINE;
    String headers =
        objectMapper.writeValueAsString(
            message.hasHeaders() ? message.getHeaders().toMap() : Map.of());
    transactions.execute(
        db -> {
          try (var insert =
              db.prepareStatement(
                  """
                  INSERT INTO knd_quarantine
                    (consumer_name,stream_name,stream_sequence,subject,event_id,payload_hash,
                     payload,headers_json,reason,disposition,delivery_count)
                  VALUES (?,?,?,?,?,?,?,?::jsonb,?,?,?) ON CONFLICT DO NOTHING
                  """)) {
            insert.setString(1, consumer);
            insert.setString(2, metadata.getStream());
            insert.setLong(3, metadata.streamSequence());
            insert.setString(4, message.getSubject());
            insert.setObject(5, eventId(message.getData()));
            insert.setBytes(6, hash);
            insert.setBytes(7, retain ? message.getData() : null);
            insert.setString(8, retain ? headers : null);
            insert.setString(9, reason);
            insert.setString(10, retain ? "quarantine" : "drop");
            insert.setLong(11, metadata.deliveredCount());
            insert.executeUpdate();
          }
          try (var existing =
              db.prepareStatement(
                  """
                  SELECT payload_hash,subject,payload IS NOT NULL FROM knd_quarantine
                  WHERE consumer_name=? AND stream_name=? AND stream_sequence=?
                  """)) {
            existing.setString(1, consumer);
            existing.setString(2, metadata.getStream());
            existing.setLong(3, metadata.streamSequence());
            try (var result = existing.executeQuery()) {
              if (!result.next()
                  || !MessageDigest.isEqual(hash, result.getBytes(1))
                  || !message.getSubject().equals(result.getString(2))) {
                throw new IllegalStateException("Recovery identity reused with different content");
              }
              if (retain && !result.getBoolean(3)) {
                // A prior discard can have committed before its ACK was lost. A newly selected
                // quarantine policy must not acknowledge that redelivery without a recovery copy.
                throw new IllegalStateException("Existing discard audit has no quarantine copy");
              }
            }
          }
          return null;
        });
  }

  /** Metadata only; full payloads are intentionally absent from the inspection command. */
  public List<Map<String, Object>> pending(int limit) throws Exception {
    if (limit < 1 || limit > 1000) {
      throw new IllegalArgumentException("List limit must be 1..1000");
    }
    return transactions.execute(
        db -> {
          var rows = new ArrayList<Map<String, Object>>();
          try (var query =
              db.prepareStatement(
                  """
                  SELECT stream_name,stream_sequence,subject,reason,delivery_count,recorded_at
                  FROM knd_quarantine WHERE consumer_name=? AND disposition='quarantine'
                    AND replayed_at IS NULL ORDER BY recorded_at LIMIT ?
                  """)) {
            query.setString(1, consumer);
            query.setInt(2, limit);
            try (var result = query.executeQuery()) {
              while (result.next()) {
                rows.add(
                    Map.of(
                        "stream",
                        result.getString(1),
                        "sequence",
                        result.getLong(2),
                        "subject",
                        result.getString(3),
                        "reason",
                        result.getString(4),
                        "deliveries",
                        result.getLong(5),
                        "recordedAt",
                        result.getString(6)));
              }
            }
          }
          return List.copyOf(rows);
        });
  }

  /**
   * Replays unchanged bytes and the original ID. A deduplicated publish is NOT proof of recovery:
   * the acknowledged original may already be gone from a WorkQueue stream. Retry after the window.
   */
  public boolean replay(JetStream jetStream, String stream, long sequence) throws Exception {
    return transactions.execute(
        db -> {
          try (var query =
              db.prepareStatement(
                  """
                  SELECT subject,payload,headers_json FROM knd_quarantine
                  WHERE consumer_name=? AND stream_name=? AND stream_sequence=?
                    AND disposition='quarantine' AND replayed_at IS NULL FOR UPDATE
                  """)) {
            query.setString(1, consumer);
            query.setString(2, stream);
            query.setLong(3, sequence);
            try (var result = query.executeQuery()) {
              if (!result.next()) {
                throw new IllegalArgumentException(
                    "No pending quarantine record for this identity");
              }
              byte[] payload = result.getBytes(2);
              Map<String, List<String>> saved =
                  objectMapper.readValue(
                      result.getString(3), new TypeReference<Map<String, List<String>>>() {});
              Headers headers = new Headers().put(saved);
              String originalId = headers.getFirst("Nats-Msg-Id");
              if (originalId == null) {
                UUID id = eventId(payload);
                originalId =
                    id == null
                        ? "quarantine-" + consumer + "-" + stream + "-" + sequence
                        : id.toString();
              }
              // Expectation headers govern one publication, not the identity of the event.
              var expectations =
                  headers.keySet().stream()
                      .filter(key -> key.regionMatches(true, 0, "Nats-Expected-", 0, 14))
                      .toList();
              headers.remove(expectations);
              var ack =
                  jetStream.publish(
                      result.getString(1),
                      headers,
                      payload,
                      PublishOptions.builder()
                          .expectedStream(stream)
                          .messageId(originalId)
                          .build());
              if (!stream.equals(ack.getStream()) || ack.getSeqno() < 1) {
                throw new IllegalStateException("Unexpected replay acknowledgement");
              }
              if (ack.isDuplicate()) {
                return false;
              }
            }
          }
          markReplayed(db, stream, sequence);
          return true;
        });
  }

  private void markReplayed(Connection db, String stream, long sequence) throws Exception {
    try (var update =
        db.prepareStatement(
            """
        UPDATE knd_quarantine SET replayed_at=CURRENT_TIMESTAMP
        WHERE consumer_name=? AND stream_name=? AND stream_sequence=?
        """)) {
      update.setString(1, consumer);
      update.setString(2, stream);
      update.setLong(3, sequence);
      update.executeUpdate();
    }
  }

  private static UUID eventId(byte[] payload) {
    try {
      var event = objectMapper.readTree(payload);
      return UUID.fromString(event.path("id").asText());
    } catch (Exception ignored) {
      return null;
    }
  }
}
