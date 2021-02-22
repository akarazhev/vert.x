package io.vertx.core.net.impl.pool;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class NonBlockingSynchronization1<S> implements Synchronization<S> {

  private final ConcurrentLinkedDeque<Action<S>> q = new ConcurrentLinkedDeque<>();
  private final AtomicInteger s = new AtomicInteger();
  private final S state;

  public NonBlockingSynchronization1(S state) {
    this.state = state;
  }

  @Override
  public void execute(Action<S> action) {
    q.add(action);
    if (s.incrementAndGet() == 1) {
      while (true) {
        int cnt = 0;
        Action<S> a;
        while ((a = q.poll()) != null) {
          cnt++;
          Runnable post = a.execute(state);
          if (post != null) {
            post.run();
          }
        }
        if (s.addAndGet(-cnt) == 0) {
          break;
        }
      }
    }
  }
}
