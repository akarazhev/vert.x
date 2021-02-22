package io.vertx.core.net.impl.pool;

public interface Synchronization<S> {

  interface Action<S> {
    Runnable execute(S state);
  }

  void execute(Action<S> action);

}
