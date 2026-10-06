package io.github.gbeaule.keycloaknats;

/**
 * Bounds relay database work without changing request transactions or the shared connection pool.
 */
public record RelayDatabaseConfig(
    int transactionTimeoutSeconds, int statementTimeoutMillis, int lockTimeoutMillis) {
  /** The transaction budget must also accommodate the bounded broker operation. */
  public RelayDatabaseConfig {
    if (transactionTimeoutSeconds < 1 || transactionTimeoutSeconds > 3605) {
      throw new IllegalArgumentException("relay-transaction-timeout-seconds must be 1..3605");
    }
    if (statementTimeoutMillis < 1
        || statementTimeoutMillis > 60000
        || statementTimeoutMillis > transactionTimeoutSeconds * 1000) {
      throw new IllegalArgumentException(
          "relay-statement-timeout-ms must be 1..60000 and within the transaction timeout");
    }
    if (lockTimeoutMillis < 1 || lockTimeoutMillis > statementTimeoutMillis) {
      throw new IllegalArgumentException(
          "relay-lock-timeout-ms must be positive and within the statement timeout");
    }
  }
}
