package io.vertx.core.net.impl.pool;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockSynchronization<S> implements Synchronization<S> {

  private final Lock lock = new ReentrantLock();
  private final S state;

  public LockSynchronization(S state) {
    this.state = state;
  }

  @Override
  public void execute(Action<S> action) {
    lock.lock();
    Runnable post = null;
    try {
      post = action.execute(state);
    } finally {
      lock.unlock();
      if (post != null) {
        post.run();
      }
    }
  }
}
