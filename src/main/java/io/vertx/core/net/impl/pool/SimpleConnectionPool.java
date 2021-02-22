/*
 * Copyright (c) 2011-2021 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.net.impl.pool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.ConnectionPoolTooBusyException;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.net.impl.clientconnection.ConnectResult;
import io.vertx.core.net.impl.clientconnection.Lease;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;

public class SimpleConnectionPool<C> implements ConnectionPool<C> {

  static class Slot<C> implements ConnectionEventListener {

    private final SimpleConnectionPool<C> pool;
    private final EventLoopContext context;
    private final Promise<C> result;
    private C connection;
    private int index;
    private int capacity;
    private int maxCapacity;
    private int weight;

    public Slot(SimpleConnectionPool<C> pool, EventLoopContext context, int index, int initialWeight) {
      this.pool = pool;
      this.context = context;
      this.connection = null;
      this.capacity = 0;
      this.index = index;
      this.weight = initialWeight;
      this.result = context.promise();
    }

    @Override
    public void remove() {
      pool.remove(this);
    }
  }

  static class Waiter<C> {

    final EventLoopContext context;
    final int weight;
    final Handler<AsyncResult<Lease<C>>> handler;

    Waiter(EventLoopContext context, final int weight, Handler<AsyncResult<Lease<C>>> handler) {
      this.context = context;
      this.weight = weight;
      this.handler = handler;
    }
  }

  private final Connector<C> connector;

  private final Slot<C>[] slots;
  private int size;
  private final Deque<Waiter<C>> waiters = new ArrayDeque<>();
  private final int maxWaiters;
  private final int maxWeight;
  private int weight;
  private boolean closed;
  private final Synchronization<SimpleConnectionPool<C>> sync;

  SimpleConnectionPool(Connector<C> connector, int maxSize, int maxWeight) {
    this(connector, maxSize, maxWeight, -1);
  }

  SimpleConnectionPool(Connector<C> connector, int maxSize, int maxWeight, int maxWaiters) {
    this.connector = connector;
    this.slots = new Slot[maxSize];
    this.size = 0;
    this.maxWaiters = maxWaiters;
    this.weight = 0;
    this.maxWeight = maxWeight;
    this.sync = new NonBlockingSynchronization2<>(this);
  }

  private void execute(Synchronization.Action<SimpleConnectionPool<C>> action) {
    sync.execute(action);
  }

  public int size() {
//    lock.lock();
//    try {
      return size;
//    } finally {
//      lock.unlock();
//    }
  }

  public void connect(Slot<C> slot, Handler<AsyncResult<Lease<C>>> handler) {
    connector.connect(slot.context, slot, ar -> {
      if (ar.succeeded()) {
        execute(new ConnectSuccess<>(slot, ar.result(), handler));
      } else {
        execute(new ConnectFailed(slot, ar.cause(), handler));
      }
    });
  }

  private static class ConnectSuccess<C> implements Synchronization.Action<SimpleConnectionPool<C>> {

    private final Slot<C> slot;
    private final ConnectResult<C> result;
    private final Handler<AsyncResult<Lease<C>>> handler;

    private ConnectSuccess(Slot<C> slot, ConnectResult<C> result, Handler<AsyncResult<Lease<C>>> handler) {
      this.slot = slot;
      this.result = result;
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      int initialWeight = slot.weight;
      slot.connection = result.connection();
      slot.maxCapacity = (int)result.concurrency();
      slot.weight = (int) result.weight();
      slot.capacity = slot.maxCapacity;
      pool.weight += (result.weight() - initialWeight);
      if (pool.closed) {
        return () -> {
          slot.context.emit(Future.failedFuture("Closed"), handler);
          slot.result.complete(slot.connection);
        };
      } else {
        int c = 1;
        LeaseImpl<C>[] extra;
        int m = Math.min(slot.capacity - 1, pool.waiters.size());
        if (m > 0) {
          c += m;
          extra = new LeaseImpl[m];
          for (int i = 0;i < m;i++) {
            extra[i] = new LeaseImpl<>(slot, pool.waiters.poll().handler);
          }
        } else {
          extra = null;
        }
        slot.capacity -= c;
        return () -> {
          new LeaseImpl<>(slot, handler).emit();
          if (extra != null) {
            for (LeaseImpl<C> lease : extra) {
              lease.emit();
            }
          }
          slot.result.complete(slot.connection);
        };
      }
    }
  }

  private static class ConnectFailed<C> extends Remove<C> {

    private final Throwable cause;
    private final Handler<AsyncResult<Lease<C>>> handler;

    public ConnectFailed(Slot<C> removed, Throwable cause, Handler<AsyncResult<Lease<C>>> handler) {
      super(removed);
      this.cause = cause;
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      Runnable res = super.execute(pool);
      return () -> {
        if (res != null) {
          res.run();
        }
        removed.context.emit(Future.failedFuture(cause), handler);
        removed.result.fail(cause);
      };
    }
  }

  private static class Remove<C> implements Synchronization.Action<SimpleConnectionPool<C>> {

    protected final Slot<C> removed;

    private Remove(Slot<C> removed) {
      this.removed = removed;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      int w = removed.weight;
      removed.capacity = 0;
      removed.maxCapacity = 0;
      removed.connection = null;
      removed.weight = 0;
      Waiter<C> waiter = pool.waiters.poll();
      if (waiter != null) {
        Slot<C> slot = new Slot<>(pool, waiter.context, removed.index, waiter.weight);
        pool.weight -= w;
        pool.weight += waiter.weight;
        pool.slots[removed.index] = slot;
        return () -> pool.connect(slot, waiter.handler);
      } else if (pool.size > 1) {
        Slot<C> tmp = pool.slots[pool.size - 1];
        tmp.index = removed.index;
        pool.slots[removed.index] = tmp;
        pool.slots[pool.size - 1] = null;
        pool.size--;
        pool.weight -= w;
        return null;
      } else {
        pool.slots[0] = null;
        pool.size--;
        pool.weight -= w;
        return null;
      }
    }
  }

  private void remove(Slot<C> removed) {
    execute(new Remove<>(removed));
  }

  private static class Evict<C> implements Synchronization.Action<SimpleConnectionPool<C>> {

    private final Predicate<C> predicate;
    private final Handler<AsyncResult<List<C>>> handler;

    public Evict(Predicate<C> predicate, Handler<AsyncResult<List<C>>> handler) {
      this.predicate = predicate;
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      List<C> lst = new ArrayList<>();
      for (int i = pool.size - 1;i >= 0;i--) {
        Slot<C> slot = pool.slots[i];
        if (slot.connection != null && slot.capacity == slot.maxCapacity && predicate.test(slot.connection)) {
          lst.add(slot.connection);
          slot.capacity = 0;
          slot.maxCapacity = 0;
          slot.connection = null;
          if (i == pool.size - 1) {
            pool.slots[i] = null;
          } else {
            Slot<C> last = pool.slots[pool.size - 1];
            last.index = i;
            pool.slots[i] = last;
          }
          pool.weight -= slot.weight;
          pool.size--;
        }
      }
      return () -> handler.handle(Future.succeededFuture(lst));
    }
  }

  @Override
  public void evict(Predicate<C> predicate, Handler<AsyncResult<List<C>>> handler) {
    execute(new Evict<>(predicate, handler));
  }

  private static class Acquire<C> implements Synchronization.Action<SimpleConnectionPool<C>> {

    private final EventLoopContext context;
    private final int weight;
    private final Handler<AsyncResult<Lease<C>>> handler;

    public Acquire(EventLoopContext context, int weight, Handler<AsyncResult<Lease<C>>> handler) {
      this.context = context;
      this.weight = weight;
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      if (pool.closed) {
        return () -> context.emit(Future.failedFuture("Closed"), handler);
      }

      // 1. Try reuse a existing connection with the same context
      for (int i = 0;i < pool.size;i++) {
        Slot<C> slot = pool.slots[i];
        if (slot != null && slot.context == context && slot.capacity > 0) {
          slot.capacity--;
          return () -> new LeaseImpl<>(slot, handler).emit();
        }
      }

      // 2. Try create connection
      if (pool.weight < pool.maxWeight) {
        pool.weight += weight;
        if (pool.size < pool.slots.length) {
          Slot<C> slot = new Slot<>(pool, context, pool.size, weight);
          pool.slots[pool.size++] = slot;
          return () -> pool.connect(slot, handler);
        } else {
          throw new IllegalStateException();
        }
      }

      // 3. Try use another context
      for (Slot<C> slot : pool.slots) {
        if (slot != null && slot.capacity > 0) {
          slot.capacity--;
          return () -> new LeaseImpl<>(slot, handler).emit();
        }
      }

      // 4. Fall in waiters list
      if (pool.maxWaiters == -1 || pool.waiters.size() < pool.maxWaiters) {
        pool.waiters.add(new Waiter<>(context, weight, handler));
        return null;
      } else {
        return () -> context.emit(Future.failedFuture(new ConnectionPoolTooBusyException("Connection pool reached max wait queue size of " + pool.maxWaiters)), handler);
      }
    }
  }

  public void acquire(EventLoopContext context, int weight, Handler<AsyncResult<Lease<C>>> handler) {
    execute(new Acquire<>(context, weight, handler));
  }

  static class LeaseImpl<C> implements Lease<C> {

    private final Handler<AsyncResult<Lease<C>>> handler;
    private final Slot<C> slot;
    private final C connection;
    private boolean recycled;

    public LeaseImpl(Slot<C> slot, Handler<AsyncResult<Lease<C>>> handler) {
      this.handler = handler;
      this.slot = slot;
      this.connection = slot.connection;
    }

    @Override
    public C get() {
      return connection;
    }

    @Override
    public void recycle() {
      slot.pool.recycle(this);
    }

    void emit() {
      slot.context.emit(Future.succeededFuture(new LeaseImpl<>(slot, handler)), handler);
    }
  }

  private static class Recycle<C> implements Synchronization.Action<SimpleConnectionPool<C>> {

    private final Slot<C> slot;

    public Recycle(Slot<C> slot) {
      this.slot = slot;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      if (slot.connection != null) {
        if (pool.waiters.size() > 0) {
          Waiter<C> waiter = pool.waiters.poll();
          return () -> new LeaseImpl<>(slot, waiter.handler).emit();
        } else {
          slot.capacity++;
          return null;
        }
      } else {
        return null;
      }
    }
  }

  private void recycle(LeaseImpl<C> lease) {
    if (lease.recycled) {
      throw new IllegalStateException("Attempt to recycle more than permitted");
    }
    lease.recycled = true;
    execute(new Recycle<>(lease.slot));
  }

  public int waiters() {
    // lock.lock();
    // try {
      return waiters.size();
    // } finally {
    //   lock.unlock();
    // }
  }

  public int weight() {
    // lock.lock();
    // try {
      return weight;
    // } finally {
    //  lock.unlock();
    // }
  }

  private static class Close<C> implements Synchronization.Action<SimpleConnectionPool<C>> {

    private final Handler<AsyncResult<List<Future<C>>>> handler;

    private Close(Handler<AsyncResult<List<Future<C>>>> handler) {
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      List<Future<C>> list;
      List<Waiter<C>> b;
      if (pool.closed) {
        throw new IllegalStateException();
      }
      pool.closed = true;
      b = new ArrayList<>(pool.waiters);
      pool.waiters.clear();
      list = new ArrayList<>();
      for (int i = 0;i < pool.size;i++) {
        list.add(pool.slots[i].result.future());
      }
      return () -> {
        b.forEach(w -> w.context.emit(Future.failedFuture("Closed"), w.handler));
        handler.handle(Future.succeededFuture(list));
      };
    }
  }

  @Override
  public void close(Handler<AsyncResult<List<Future<C>>>> handler) {
    execute(new Close<>(handler));
  }
}
