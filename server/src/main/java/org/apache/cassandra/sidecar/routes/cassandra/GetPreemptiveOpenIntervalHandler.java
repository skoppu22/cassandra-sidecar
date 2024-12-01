/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.routes.cassandra;

import javax.inject.Singleton;

import com.google.inject.Inject;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.response.GetPreemptiveOpenIntervalResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.metrics.JmxOperationsMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Functionality to retrieve sstable's preemptive open interval value
 */
@Singleton
public class GetPreemptiveOpenIntervalHandler extends AbstractHandler<Void>
{
    private final JmxOperationsMetrics jmxOperationsMetrics;

    @Inject
    protected GetPreemptiveOpenIntervalHandler(InstanceMetadataFetcher metadataFetcher,
                                               ExecutorPools executorPools,
                                               CassandraInputValidator validator,
                                               SidecarMetrics sidecarMetrics)
    {
        super(metadataFetcher, executorPools, validator);
        this.jmxOperationsMetrics = sidecarMetrics.server().jmxOperationsMetrics();
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  Void request)
    {
        getSSTablePreemptiveOpenInterval(host, remoteAddress)
        .onSuccess(context::json)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    protected Future<GetPreemptiveOpenIntervalResponse> getSSTablePreemptiveOpenInterval(String host, SocketAddress remoteAddress)
    {
        long startTime = System.nanoTime();
        StorageOperations storageOperations = getStorageOperations(host);
        logger.debug("Retrieving SSTable's preemptiveOpenInterval, remoteAddress={}, instance={}",
                     remoteAddress, host);

        return executorPools.service()
                            .executeBlocking(storageOperations::getSSTablePreemptiveOpenIntervalInMB)
                            .onComplete(ar -> updateJmxMetric(ar, jmxOperationsMetrics, "getSSTablePreemptiveOpenInterval", startTime));
    }
}
