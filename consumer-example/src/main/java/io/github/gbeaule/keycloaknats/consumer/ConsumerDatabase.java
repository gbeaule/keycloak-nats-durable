package io.github.gbeaule.keycloaknats.consumer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;

/** Reuses bounded database connections instead of authenticating a new session for every event. */
public final class ConsumerDatabase {
  static final int MIN_POOL_SIZE = 1;
  static final int MAX_POOL_SIZE = 32;

  private ConsumerDatabase() {}

  /**
   * The owner must close the pool; JDBC socket and SQL deadlines still apply to every transaction.
   */
  public static HikariDataSource pool(
      PGSimpleDataSource database, ProcessingLimits limits, int size) {
    if (size < MIN_POOL_SIZE || size > MAX_POOL_SIZE) {
      throw new IllegalArgumentException("KND_CONSUMER_DB_POOL_SIZE must be 1..32");
    }
    limits.configure(database);
    var config = new HikariConfig();
    config.setPoolName("knd-consumer");
    config.setDataSource(database);
    config.setMaximumPoolSize(size);
    config.setMinimumIdle(0);
    config.setConnectionTimeout(limits.connectSeconds() * 1000L);
    config.setValidationTimeout(500);
    config.setInitializationFailTimeout(-1);
    return new HikariDataSource(config);
  }
}
