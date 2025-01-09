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

package org.apache.cassandra.sidecar.routes;

import javax.inject.Singleton;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.server.DataStorageUnit;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.RequestUtils;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Functionality to retrieve sstable's preemptive open interval value
 * unit is an optional param, only supported value is "MiB" defaults to the same if not provided
 */
@Singleton
public class GetPreemptiveOpenIntervalHandler extends AbstractHandler<DataStorageUnit>
{
    @Inject
    protected GetPreemptiveOpenIntervalHandler(InstanceMetadataFetcher metadataFetcher,
                                               ExecutorPools executorPools,
                                               CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    @Override
    protected DataStorageUnit extractParamsOrThrow(RoutingContext context)
    {
        // Only supported unit for preemptive open interval value is MB
        String unit = RequestUtils.parseStringQueryParam(context.request(), "unit", "MiB");
        if (!unit.equals(DataStorageUnit.MEBIBYTES.getValue()))
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                    String.format("Invalid value provided for unit %s, expected %s",
                                                  unit, DataStorageUnit.MEBIBYTES.getValue()));
        }

        return DataStorageUnit.MEBIBYTES;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  DataStorageUnit unit)
    {
        StorageOperations operations = metadataFetcher.delegate(host).storageOperations();
        executorPools.service()
                     .executeBlocking(() -> operations.getSSTablePreemptiveOpenInterval(unit))
                     .onSuccess(context::json)
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, unit));
    }
}
