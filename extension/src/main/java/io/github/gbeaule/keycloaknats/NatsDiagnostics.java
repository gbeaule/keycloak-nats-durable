package io.github.gbeaule.keycloaknats;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import java.util.Locale;
import org.jboss.logging.Logger;

/** Log failures without echoing remote text, event payloads, credentials or connection URLs. */
final class NatsDiagnostics implements ErrorListener, ConnectionListener {
  private static final Logger logger = Logger.getLogger(NatsDiagnostics.class);

  @Override
  public void connectionEvent(Connection connection, Events event) {
    if (event == Events.DISCONNECTED) {
      logger.warn("NATS disconnected; committed events remain in the outbox for retry");
    } else {
      logger.infof("NATS connection event=%s", event);
    }
  }

  @Override
  public void errorOccurred(Connection connection, String error) {
    logger.errorf("NATS server error; reason=%s", serverErrorCategory(error));
  }

  @Override
  public void exceptionOccurred(Connection connection, Exception exception) {
    logger.errorf("NATS client exception; %s", describe(exception));
  }

  @Override
  public void slowConsumerDetected(Connection connection, Consumer consumer) {
    logger.error("NATS slow consumer detected; publish acknowledgements may time out");
  }

  @Override
  public void messageDiscarded(Connection connection, Message message) {
    logger.error("NATS client discarded a message; unacknowledged outbox rows remain pending");
  }

  @Override
  public void socketWriteTimeout(Connection connection) {
    logger.error("NATS socket write timed out; unacknowledged outbox rows remain pending");
  }

  static String serverErrorCategory(String error) {
    String normalized = error == null ? "" : error.toLowerCase(Locale.ROOT);
    if (normalized.contains("authorization") || normalized.contains("authentication")) {
      return "authentication_rejected";
    }
    if (normalized.contains("permissions violation")) {
      return "permission_denied";
    }
    if (normalized.contains("maximum payload")) {
      return "payload_limit";
    }
    if (normalized.contains("stale connection")) {
      return "stale_connection";
    }
    return "server_error (inspect the NATS server logs)";
  }

  static String describe(Throwable failure) {
    StringBuilder detail = new StringBuilder();
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < 4; depth++) {
      if (depth > 0) {
        detail.append(" causedBy=");
      }
      detail.append(cause.getClass().getSimpleName());
      if (cause instanceof UnsafeStreamException) {
        detail.append(": ").append(cause.getMessage());
      } else if (cause instanceof JetStreamApiException api) {
        detail.append(" status=").append(api.getErrorCode());
        detail.append(" apiCode=").append(api.getApiErrorCode());
      }
      cause = cause.getCause();
    }
    return detail.toString();
  }
}
