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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JmxCommonTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(JmxCommonTest.class);
    CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
    StorageOperations storageOperations = mock(StorageOperations.class);
    Vertx vertx;
    Server server;

    @BeforeEach
    void before() throws InterruptedException
    {
        Module testOverride = Modules.override(new TestModule()).with(new JmxTestModule());
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    class JmxTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesMetadata instanceConfig()
        {
            int instanceId1 = 100;
            String host1 = "127.0.0.1";
            InstanceMetadata instanceMetadata1 = mock(InstanceMetadata.class);
            when(instanceMetadata1.host()).thenReturn(host1);
            when(instanceMetadata1.port()).thenReturn(9042);
            when(instanceMetadata1.id()).thenReturn(instanceId1);
            when(instanceMetadata1.stagingDir()).thenReturn("");

            int instanceId2 = 200;
            String host2 = "127.0.0.2";
            InstanceMetadata instanceMetadata2 = mock(InstanceMetadata.class);
            when(instanceMetadata2.host()).thenReturn(host2);
            when(instanceMetadata2.port()).thenReturn(9042);
            when(instanceMetadata2.id()).thenReturn(instanceId2);
            when(instanceMetadata2.stagingDir()).thenReturn("");

            when(delegate.storageOperations()).thenReturn(storageOperations);
            when(instanceMetadata1.delegate()).thenReturn(delegate);
            when(instanceMetadata2.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(ImmutableList.of(instanceMetadata1, instanceMetadata2));
            when(mockInstancesMetadata.instanceFromId(instanceId1)).thenReturn(instanceMetadata1);
            when(mockInstancesMetadata.instanceFromHost(host1)).thenReturn(instanceMetadata1);
            when(mockInstancesMetadata.instanceFromId(instanceId2)).thenReturn(instanceMetadata2);
            when(mockInstancesMetadata.instanceFromHost(host2)).thenReturn(instanceMetadata2);

            return mockInstancesMetadata;
        }
    }
}
