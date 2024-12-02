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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.GetPreemptiveOpenIntervalResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.metrics.JmxOperationsMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Functionality to update C* sstable_preemptive_open_interval value
 */
@Singleton
public class SetPreemptiveOpenIntervalHandler extends AbstractHandler<Integer>
{
    private final JmxOperationsMetrics jmxOperationsMetrics;

    @Inject
    protected SetPreemptiveOpenIntervalHandler(InstanceMetadataFetcher metadataFetcher,
                                               ExecutorPools executorPools,
                                               CassandraInputValidator validator,
                                               SidecarMetrics sidecarMetrics)
    {
        super(metadataFetcher, executorPools, validator);
        this.jmxOperationsMetrics = sidecarMetrics.server().jmxOperationsMetrics();
    }

    @Override
    protected Integer extractParamsOrThrow(RoutingContext context)
    {
        Integer preemptiveOpenIntervalInMB = parseIntegerPathParam(context, "preemptiveOpenInterval");
        // Validate acceptable range for preemptiveOpenIntervalInMB
        // -1 means disabled. Quantity provided is in MB, and cannot exceed Integer.MAX_VALUE - 1
        if (!(preemptiveOpenIntervalInMB == -1 ||
             (preemptiveOpenIntervalInMB > 0 && preemptiveOpenIntervalInMB < Integer.MAX_VALUE - 1)))
        {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                    "Invalid value provided for preemptiveOpenInterval: " + preemptiveOpenIntervalInMB);
        }
        return preemptiveOpenIntervalInMB;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  Integer preemptiveOpenIntervalInMB)
    {
        setSSTablePreemptiveOpenInterval(host, remoteAddress, preemptiveOpenIntervalInMB)
        .onSuccess(context::json) // return updated value
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, preemptiveOpenIntervalInMB));
    }

    protected Future<GetPreemptiveOpenIntervalResponse> setSSTablePreemptiveOpenInterval(String host,
                                                                                         SocketAddress remoteAddress,
                                                                                         Integer preemptiveOpenIntervalInMB)
    {
        long startTime = System.nanoTime();
        StorageOperations storageOperations = getStorageOperations(host);
        logger.debug("Updating SSTable's preemptiveOpenInterval to value {} mb, remoteAddress={}, instance={}",
                     preemptiveOpenIntervalInMB,
                     remoteAddress, host);

        return executorPools.service()
                            .executeBlocking(() -> storageOperations.setSSTablePreemptiveOpenIntervalInMB(preemptiveOpenIntervalInMB))
                            .onComplete(ar -> updateJmxMetric(ar, jmxOperationsMetrics, "setSSTablePreemptiveOpenInterval", startTime));
    }
}
