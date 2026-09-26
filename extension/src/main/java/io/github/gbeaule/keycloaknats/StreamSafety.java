package io.github.gbeaule.keycloaknats;

import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import java.util.List;

/** Fail closed rather than send into a stream that may silently evict unprocessed events. */
public final class StreamSafety {
  private StreamSafety() {}

  /** Rejects stream settings that can silently expire, evict or reroute a retained event. */
  public static void validate(StreamConfiguration stream, BridgeConfig config) {
    require(config.stream().equals(stream.getName()), "Unexpected stream");
    require(
        stream.getSubjects().equals(List.of(config.subjectPrefix() + ".>")),
        "Stream must own the configured subject prefix exclusively");
    require(stream.getStorageType() == StorageType.File, "File storage is required");
    require(stream.getReplicas() >= config.minReplicas(), "Insufficient stream replicas");
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
            || stream.getMaximumMessageSize() >= config.maxPayloadBytes() + 512,
        "Stream message size must leave room for the payload and headers");
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new UnsafeStreamException(message);
    }
  }
}
