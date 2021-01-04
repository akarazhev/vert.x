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

/**
 * Connection pool.
 */
public interface ConnectionPool<C> {

  /**
   * Acquire a connection from the pool.
   *
   * @param context the context
   * @param weight the weight
   * @param handler the callback
   */
  void acquire(EventLoopContext context, int weight, Handler<AsyncResult<Lease<C>>> handler);

  /**
   * <p> Evict connections from the pool with a predicate, only unused connection are evicted.
   *
   * <p> The operation returns the list of connections that won't be managed anymore by the pool.
   *
   * @param predicate to determine whether a connection should be evicted
   * @return the list of evicted connections
   */
  List<C> evict(Predicate<C> predicate);

  /**
   * Close the pool.
   *
   * <p> This will not close the connections, instead a list of connections to be closed is returned.
   *
   * @return the list of connections to close
   */
  List<Future<C>> close();

  /**
   * @return the number of managed connections
   */
  int size();

  /**
   * @return the number of waiters
   */
  int waiters();

  /**
   * @return the pool weight
   */
  int weight();

}
