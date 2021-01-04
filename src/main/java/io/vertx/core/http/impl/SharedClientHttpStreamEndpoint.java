/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.http.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.net.impl.clientconnection.ConnectResult;
import io.vertx.core.net.impl.pool.ConnectionEventListener;
import io.vertx.core.net.impl.pool.ConnectionPool;
import io.vertx.core.net.impl.pool.Connector;
import io.vertx.core.net.impl.clientconnection.Lease;
import io.vertx.core.net.impl.pool.SimpleConnectionPool;
import io.vertx.core.spi.metrics.ClientMetrics;

import java.util.List;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
class SharedClientHttpStreamEndpoint extends ClientHttpEndpointBase<Lease<HttpClientConnection>> implements Connector<HttpClientConnection> {

  private final HttpClientImpl client;
  private final HttpChannelConnector connector;
  private final ConnectionPool<HttpClientConnection> pool;
  private final int http1MaxSize;
  private final int http2MaxSize;

  public SharedClientHttpStreamEndpoint(HttpClientImpl client,
                                        ClientMetrics metrics,
                                        Object metric,
                                        int queueMaxSize,
                                        int http1MaxSize,
                                        int http2MaxSize,
                                        String host,
                                        int port,
                                        HttpChannelConnector connector,
                                        Runnable dispose) {
    super(metrics, port, host, metric, dispose);
    this.client = client;
    this.connector = connector;
    this.http1MaxSize = http1MaxSize;
    this.http2MaxSize = http2MaxSize;
    this.pool = new SimpleConnectionPool<>(this, Math.max(http1MaxSize, http2MaxSize), http1MaxSize * http2MaxSize, queueMaxSize);
  }

  @Override
  public void connect(EventLoopContext context, ConnectionEventListener listener, Handler<AsyncResult<ConnectResult<HttpClientConnection>>> handler) {
    connector
      .httpConnect(context)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          incRefCount();
          HttpClientConnection connection = ar.result();
          connection.evictionHandler(v -> {
            decRefCount();
            listener.remove();
          });
          connection.concurrencyChangeHandler(concurrency -> {
            // TODO
          });
          long capacity = connection.concurrency();
          Handler<HttpConnection> connectionHandler = client.connectionHandler();
          if (connectionHandler != null) {
            context.emit(connection, connectionHandler);
          }
          int weight;
          if (connection instanceof Http1xClientConnection) {
            weight = http2MaxSize;
          } else {
            weight = http1MaxSize;
          }
          handler.handle(Future.succeededFuture(new ConnectResult<>(connection, capacity, weight)));
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
  }

  @Override
  public boolean isValid(HttpClientConnection connection) {
    return connection.isValid();
  }

  void checkExpired() {
    List<HttpClientConnection> lst = pool.evict(conn -> !conn.isValid());
    lst.forEach(HttpConnection::close);
  }

  @Override
  public void requestConnection2(ContextInternal ctx, Handler<AsyncResult<Lease<HttpClientConnection>>> handler) {
    int weight = client.getOptions().getProtocolVersion() == HttpVersion.HTTP_2 ? http1MaxSize : http2MaxSize;
    pool.acquire((EventLoopContext) ctx, weight, handler);
  }
}
