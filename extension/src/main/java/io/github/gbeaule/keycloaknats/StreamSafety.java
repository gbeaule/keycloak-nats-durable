package io.github.gbeaule.keycloaknats;

import io.github.gbeaule.keycloaknats.jetstream.StreamPolicy;
import io.nats.client.api.StreamConfiguration;

/** Fail closed rather than send into a stream that may silently evict unprocessed events. */
public final class StreamSafety {
  private StreamSafety() {}

  /** Rejects stream settings that can silently expire, evict or reroute a retained event. */
  public static void validate(StreamConfiguration stream, BridgeConfig config) {
    try {
      StreamPolicy.validate(
          stream,
          config.stream(),
          config.subjectPrefix(),
          config.minReplicas(),
          config.maxPayloadBytes());
    } catch (StreamPolicy.Violation failure) {
      throw new UnsafeStreamException(failure.getMessage());
    }
  }
}
