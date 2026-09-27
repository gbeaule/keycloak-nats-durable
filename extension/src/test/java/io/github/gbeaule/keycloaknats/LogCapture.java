package io.github.gbeaule.keycloaknats;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

final class LogCapture implements AutoCloseable {
  private final Logger logger;
  private final Level previousLevel;
  private final boolean previousParents;
  private final List<LogRecord> records = new ArrayList<>();
  private final Handler handler =
      new Handler() {
        @Override
        public void publish(LogRecord record) {
          records.add(record);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
      };

  LogCapture(Class<?> owner) {
    logger = Logger.getLogger(owner.getName());
    previousLevel = logger.getLevel();
    previousParents = logger.getUseParentHandlers();
    logger.addHandler(handler);
    logger.setLevel(Level.ALL);
    logger.setUseParentHandlers(false);
  }

  List<LogRecord> records() {
    return List.copyOf(records);
  }

  @Override
  public void close() {
    logger.removeHandler(handler);
    logger.setLevel(previousLevel);
    logger.setUseParentHandlers(previousParents);
  }
}
