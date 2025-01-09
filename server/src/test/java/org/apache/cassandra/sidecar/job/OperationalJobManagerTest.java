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

package org.apache.cassandra.sidecar.job;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.OperationalJobConflictException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests to validate the Job submission behavior for scenarios which are a combination of values for
 *
 * <ul>
 * <ol> 1) Downstream job existence,</ol>
 * <ol> 2) Cached job (null (not in cache), Completed/Failed job, Running job), and</ol>
 * <ol> 3) Request UUID (null (no header), UUID)</ol>
 * </ul>
 */
class OperationalJobManagerTest
{
    @Mock
    SidecarConfiguration mockConfig;

    protected Vertx vertx;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        MockitoAnnotations.openMocks(this);
        ServiceConfiguration mockServiceConfig = mock(ServiceConfiguration.class);
        when(mockConfig.serviceConfiguration()).thenReturn(mockServiceConfig);
        when(mockServiceConfig.operationalJobExecutionMaxWaitTimeInMillis()).thenReturn(5000L);
    }

    @Test
    void testWithNoDownstreamJob()
    {
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker);

        OperationalJob testJob = OperationalJobTest.createOperationalJob(SUCCEEDED);
        manager.trySubmitJob(testJob);
        testJob.execute(Promise.promise());
        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(SUCCEEDED);
        assertThat(tracker.get(testJob.jobId)).isNotNull();
    }

    @Test
    void testWithRunningDownstreamJob()
    {
        OperationalJob runningJob = OperationalJobTest.createOperationalJob(RUNNING);
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        ExecutorPools mockPools = mock(ExecutorPools.class);
        TaskExecutorPool mockExecPool = mock(TaskExecutorPool.class);
        when(mockPools.internal()).thenReturn(mockExecPool);
        when(mockExecPool.runBlocking(any())).thenReturn(null);
        OperationalJobManager manager = new OperationalJobManager(tracker);
        assertThatThrownBy(() -> manager.trySubmitJob(runningJob))
        .isExactlyInstanceOf(OperationalJobConflictException.class)
        .hasMessage("The same operational job is already running on Cassandra. operationName='Operation X'");
    }

    @Test
    void testWithLongRunningJob()
    {
        UUID jobId = UUIDs.timeBased();

        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker);

        OperationalJob testJob = OperationalJobTest.createOperationalJob(jobId, Duration.ofSeconds(10));

        manager.trySubmitJob(testJob);
        // execute the job async.
        vertx.executeBlocking(testJob::execute);
        // by the time of checking, the job should still be running. It runs for 10 seconds.
        assertThat(testJob.asyncResult().isComplete()).isFalse();
        assertThat(tracker.get(jobId)).isNotNull();
    }

    @Test
    void testWithFailingJob()
    {
        UUID jobId = UUIDs.timeBased();

        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker);

        String msg = "Test Job failed";
        OperationalJob failingJob = new OperationalJob(jobId)
        {
            @Override
            protected void executeInternal() throws OperationalJobException
            {
                throw new OperationalJobException(msg);
            }
        };

        manager.trySubmitJob(failingJob);
        failingJob.execute(Promise.promise());
        assertThat(failingJob.asyncResult().isComplete()).isTrue();
        assertThat(failingJob.asyncResult().failed()).isTrue();
        assertThat(tracker.get(jobId)).isNotNull();
    }
}
