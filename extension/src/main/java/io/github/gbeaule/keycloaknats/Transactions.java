package io.github.gbeaule.keycloaknats;

import jakarta.persistence.EntityManager;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/** Owns a fresh persistence context and returns only after its transaction commits. */
@FunctionalInterface
public interface Transactions {
  /** Commits the supplied work or throws if completion cannot be confirmed. */
  void run(Consumer<EntityManager> work);

  /** Exposes the work's result only after a confirmed commit. */
  default <T> T commit(Function<EntityManager, T> work) {
    var result = new AtomicReference<T>();
    run(em -> result.set(work.apply(em)));
    return result.get();
  }
}
