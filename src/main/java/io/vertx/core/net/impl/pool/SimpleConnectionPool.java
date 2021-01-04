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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
      pool.lock.lock();
      int w = weight;
      capacity = 0;
      maxCapacity = 0;
      connection = null;
      weight = 0;
      Waiter<C> waiter = pool.waiters.poll();
      if (waiter != null) {
        Slot<C> slot = new Slot<>(pool, waiter.context, index, waiter.weight);
        pool.weight -= w;
        pool.weight += waiter.weight;
        pool.slots[index] = slot;
        pool.lock.unlock();
        slot.connect(waiter.handler);
      } else if (pool.size > 1) {
        Slot<C> tmp = pool.slots[pool.size - 1];
        tmp.index = index;
        pool.slots[index] = tmp;
        pool.slots[pool.size - 1] = null;
        pool.size--;
        pool.weight -= w;
        pool.lock.unlock();
      } else {
        pool.slots[0] = null;
        pool.size--;
        pool.weight -= w;
        pool.lock.unlock();
      }
    }

    public void connect(Handler<AsyncResult<Lease<C>>> handler) {
      pool.connector.connect(context, this, ar -> {
        if (ar.succeeded()) {
          pool.lock.lock();
          ConnectResult<C> result = ar.result();
          int initialWeight = weight;
          connection = result.connection();
          maxCapacity = (int)result.concurrency();
          weight = (int) result.weight();
          capacity = maxCapacity;
          pool.weight += (result.weight() - initialWeight);
          if (pool.closed) {
            pool.lock.unlock();
            context.emit(Future.failedFuture("Closed"), handler);
          } else {
            int c = 1;
            LeaseImpl<C>[] extra = null;
            int m = Math.min(capacity - 1, pool.waiters.size());
            if (m > 0) {
              c += m;
              extra = new LeaseImpl[m];
              for (int i = 0;i < m;i++) {
                extra[i] = new LeaseImpl<>(this, pool.waiters.poll().handler);
              }
            }
            capacity -= c;
            pool.lock.unlock();
            new LeaseImpl<>(this, handler).emit();
            if (extra != null) {
              for (LeaseImpl<C> lease : extra) {
                lease.emit();
              }
            }
          }
          this.result.complete(connection);
        } else {
          remove();
          context.emit(Future.failedFuture(ar.cause()), handler);
          result.fail(ar.cause());
        }
      });
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

  private final Lock lock = new ReentrantLock();

  public SimpleConnectionPool(Connector<C> connector, int maxSize, int maxWeight) {
    this(connector, maxSize, maxWeight, -1);
  }

  public SimpleConnectionPool(Connector<C> connector, int maxSize, int maxWeight, int maxWaiters) {
    this.connector = connector;
    this.slots = new Slot[maxSize];
    this.size = 0;
    this.maxWaiters = maxWaiters;
    this.weight = 0;
    this.maxWeight = maxWeight;
  }

  public int size() {
    lock.lock();
    try {
      return size;
    } finally {
      lock.unlock();
    }
  }

  public List<C> evict(Predicate<C> predicate) {
    lock.lock();

    List<C> lst = new ArrayList<>();

    for (int i = size - 1;i >= 0;i--) {
      Slot<C> slot = slots[i];
      if (slot.connection != null && slot.capacity == slot.maxCapacity && predicate.test(slot.connection)) {
        lst.add(slot.connection);
        slot.capacity = 0;
        slot.maxCapacity = 0;
        slot.connection = null;
        if (i == size - 1) {
          slots[i] = null;
        } else {
          Slot<C> last = slots[size - 1];
          last.index = i;
          slots[i] = last;
        }
        weight -= slot.weight;
        size--;
      }
    }

    lock.unlock();

    //
    return lst;
  }

  public void acquire(EventLoopContext context, int weight, Handler<AsyncResult<Lease<C>>> handler) {

    lock.lock();

    if (closed) {
      lock.unlock();
      context.emit(Future.failedFuture("Closed"), handler);
      return;
    }

    // 1. Try reuse a existing connection with the same context
    for (int i = 0;i < size;i++) {
      Slot<C> slot = slots[i];
      if (slot != null && slot.context == context && slot.capacity > 0) {
        slot.capacity--;
        lock.unlock();
        new LeaseImpl<>(slot, handler).emit();
        return;
      }
    }

    // 2. Try create connection
    if (this.weight < maxWeight) {
      this.weight += weight;
      if (size < slots.length) {
        Slot<C> slot = new Slot<>(this, context, size, weight);
        slots[size++] = slot;
        lock.unlock();
        slot.connect(handler);
        return;
      } else {
        throw new IllegalStateException();
      }
    }

    // 3. Try use another context
    for (Slot<C> slot : slots) {
      if (slot != null && slot.capacity > 0) {
        slot.capacity--;
        lock.unlock();
        new LeaseImpl<>(slot, handler).emit();
        return;
      }
    }

    // 4. Fall in waiters list
    if (maxWaiters == -1 || waiters.size() < maxWaiters) {
      waiters.add(new Waiter<>(context, weight, handler));
      lock.unlock();
    } else {
      lock.unlock();
      context.emit(Future.failedFuture(new ConnectionPoolTooBusyException("Connection pool reached max wait queue size of " + maxWaiters)), handler);
    }
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
    public boolean recycle() {
      return slot.pool.recycle(this);
    }

    void emit() {
      slot.context.emit(Future.succeededFuture(new LeaseImpl<>(slot, handler)), handler);
    }
  }

  private boolean recycle(LeaseImpl<C> lease) {

    lock.lock();

    if (lease.recycled) {
      throw new IllegalStateException("Attempt to recycle more than permitted");
    }
    lease.recycled = true;

    Slot slot = lease.slot;

    if (slot.connection != null) {
      if (waiters.size() > 0) {
        Waiter<C> waiter = waiters.poll();
        lock.unlock();
        new LeaseImpl<>(slot, waiter.handler).emit();
      } else {
        slot.capacity++;
        lock.unlock();
      }
      return true;
    } else {
      lock.unlock();
      return false;
    }
  }

  public int waiters() {
    lock.lock();
    try {
      return waiters.size();
    } finally {
      lock.unlock();
    }
  }

  public int weight() {
    lock.lock();
    try {
      return weight;
    } finally {
      lock.unlock();
    }
  }

  public List<Future<C>> close() {
    List<Future<C>> list;
    List<Waiter<C>> b;
    lock.lock();
    try {
      if (closed) {
        throw new IllegalStateException();
      }
      closed = true;
      b = new ArrayList<>(waiters);
      waiters.clear();
      list = new ArrayList<>();
      for (int i = 0;i < size;i++) {
        list.add(slots[i].result.future());
      }
    } finally {
      lock.unlock();
    }
    b.forEach(w -> w.context.emit(Future.failedFuture("Closed"), w.handler));
    return list;
  }
}
