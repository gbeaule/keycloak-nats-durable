package io.github.gbeaule.keycloaknats.consumer;

import java.util.function.Function;
import org.postgresql.ds.PGSimpleDataSource;

/** Bounded database and handler waits. Values are milliseconds unless their name says seconds. */
public record ProcessingLimits(
    int connectSeconds,
    int socketSeconds,
    int statementMs,
    int lockMs,
    int deadlineMs,
    int cleanupMs) {

  /** Rejects disabled deadlines and unreasonable waits. */
  public ProcessingLimits {
    if (connectSeconds < 1
        || connectSeconds > 300
        || socketSeconds < 1
        || socketSeconds > 3600
        || statementMs < 1
        || statementMs > 3600000
        || lockMs < 1
        || lockMs > statementMs
        || deadlineMs < 1
        || deadlineMs > 3600000
        || cleanupMs < 1
        || cleanupMs > 30000) {
      throw new IllegalArgumentException("Invalid consumer processing limits");
    }
  }

  /** Defaults leave time for an ACK inside the example consumer's thirty-second AckWait. */
  public static ProcessingLimits defaults() {
    return from(name -> null);
  }

  /**
   * Reads explicitly bounded settings without including credentials in configuration diagnostics.
   */
  public static ProcessingLimits from(Function<String, String> environment) {
    return new ConsumerConfig(environment).processing();
  }

  /** Explicit settings override timeout properties embedded in the database URL. */
  public void configure(PGSimpleDataSource database) {
    database.setConnectTimeout(connectSeconds);
    database.setLoginTimeout(connectSeconds);
    database.setSocketTimeout(socketSeconds);
    database.setQueryTimeout(Math.max(1, (statementMs + 999) / 1000));
    database.setTcpKeepAlive(true);
  }
}
