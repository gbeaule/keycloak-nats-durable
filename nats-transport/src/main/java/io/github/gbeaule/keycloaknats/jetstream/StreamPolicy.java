package io.github.gbeaule.keycloaknats.jetstream;

import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.util.List;

/** Shared publication and operator-replay checks against silent eviction or rerouting. */
public final class StreamPolicy {
  private static final long HEADER_ALLOWANCE = 512;

  private StreamPolicy() {}

  /** A fixed diagnostic, safe to report without printing remote server text. */
  public static final class Violation extends IllegalStateException {
    private Violation(String reason) {
      super(reason);
    }
  }

  /** Validates the expected destination and durability contract before publication. */
  public static void validate(
      StreamConfiguration stream, String name, String prefix, int replicas, int payloadBytes) {
    require(name.equals(stream.getName()), "Unexpected stream");
    require(
        stream.getSubjects().equals(List.of(prefix + ".>")),
        "Stream must own the configured subject prefix exclusively");
    require(stream.getStorageType() == StorageType.File, "File storage is required");
    require(stream.getReplicas() >= replicas, "Insufficient stream replicas");
    require(
        stream.getRetentionPolicy() == RetentionPolicy.WorkQueue
            || stream.getRetentionPolicy() == RetentionPolicy.Limits,
        "Interest retention can lose events without consumers");
    require(stream.getDiscardPolicy() == DiscardPolicy.New, "DiscardNew is required");
    require(stream.getMaxAge().isZero(), "Stream expiry must be disabled");
    require(stream.getMaxMsgsPerSubject() < 0, "Per-subject limits must be disabled");
    require(!stream.getNoAck(), "Publish acknowledgements are required");
    require(
        !stream.getAllowRollup()
            && !stream.getAllowMessageTtl()
            && !stream.getAllowMsgSchedules()
            && !stream.getAllowMessageCounter(),
        "Rollup, TTL, scheduling and counter modes must be disabled");
    require(
        !stream.getSealed()
            && stream.getMirror() == null
            && stream.getSubjectTransform() == null
            && stream.getSources().isEmpty(),
        "Stream must accept untransformed local publications");
    require(!stream.getDuplicateWindow().isZero(), "A deduplication window is required");
    require(
        stream.getMaximumMessageSize() < 0
            || stream.getMaximumMessageSize() >= payloadBytes + HEADER_ALLOWANCE,
        "Stream message size must leave room for the payload and headers");
  }

  /** The server's negotiated message limit includes JetStream and application headers. */
  public static void validateServerPayload(long serverMaxPayload, int payloadBytes) {
    require(
        serverMaxPayload >= payloadBytes + HEADER_ALLOWANCE,
        "Server max_payload must leave room for the configured payload and headers");
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new Violation(message);
    }
  }
}
