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

package org.apache.cassandra.sidecar.routes.restore;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse.ReplicaInfo;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobProgressResponsePayload;
import org.apache.cassandra.sidecar.common.response.data.RestoreRangeJson;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.restore.RestoreRangeTest;

import static org.apache.cassandra.sidecar.db.RestoreJobTest.createTestingJob;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class RestoreJobProgressHandlerTest extends BaseRestoreJobTests
{
    private static final String RESTORE_JOB_PROGRESS_ENDPOINT = "/api/v1/keyspaces/%s/tables/%s/restore-jobs/%s/progress";
    private static final String TEST_JOB_ID = "8e5799a4-d277-11ed-8d85-6916bb9b8056";
    private static final String TEST_PROGRESS_ROUTE = String.format(RESTORE_JOB_PROGRESS_ENDPOINT, "ks", "table", TEST_JOB_ID);

    @Test
    void testRejectSparkManagedRestoreJob(VertxTestContext context)
    {
        mockLookupRestoreJob(jobId -> createTestingJob(jobId, RestoreJobStatus.CREATED));
        getThenComplete(context, TEST_PROGRESS_ROUTE,
                        asyncResult -> assertStatusAndErrorMessage(asyncResult, HttpResponseStatus.BAD_REQUEST,
                                                                   "Only Sidecar-managed restore jobs are allowed. " +
                                                                   "jobId=8e5799a4-d277-11ed-8d85-6916bb9b8056 jobManager=SPARK"));
    }

    @Test
    void testRejectQueryProgressForRestoreJobInCreated(VertxTestContext context)
    {
        mockLookupRestoreJob(jobId -> createTestingJob(jobId, RestoreJobStatus.CREATED, ConsistencyLevel.QUORUM));
        getThenComplete(context, TEST_PROGRESS_ROUTE,
                        asyncResult -> assertStatusAndErrorMessage(asyncResult, HttpResponseStatus.BAD_REQUEST,
                                                                   "Cannot check progress for restore job in CREATED status. " +
                                                                   "jobId: " + TEST_JOB_ID));
    }

    @Test
    void testRetrievePendingProgressWhenFindingNoRanges(VertxTestContext context)
    {
        mockLookupRestoreJob(jobId -> createTestingJob(jobId, RestoreJobStatus.STAGE_READY, ConsistencyLevel.QUORUM));
        mockTopologyInRefresher(() -> generateTestTopology(3));
        mockFindAllRestoreRanges(jobId -> Collections.emptyList()); // there are no ranges found for the job
        getThenComplete(context, TEST_PROGRESS_ROUTE,
                        asyncResult -> {
                            RestoreJobProgressResponsePayload respBody = assertOKResponseAndExtractBody(asyncResult);
                            assertPendingProgressRespBody(respBody);
                            assertThat(sidecarMetrics.server().restore().consistencyCheckTime.metric.getSnapshot().getValues()).hasSize(1);
                        });
    }

    @Test
    void testRetrieveProgressFailsWhenFailsToReadDatabase(VertxTestContext context)
    {
        mockLookupRestoreJob(jobId -> createTestingJob(jobId, RestoreJobStatus.STAGE_READY, ConsistencyLevel.QUORUM));
        mockTopologyInRefresher(() -> generateTestTopology(3));
        mockFindAllRestoreRanges(jobId -> {
            throw new RuntimeException("Failed to read from Cassandra");
        });
        getThenComplete(context, TEST_PROGRESS_ROUTE,
                        asyncResult -> assertStatusAndErrorMessage(asyncResult, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                   // todo: should we expose the actual cause to client?
                                                                   "Unexpected error encountered in handler"));
    }

    @Test
    void testRetrieveProgressFailsWhenTopologyNotLoading(VertxTestContext context)
    {
        mockLookupRestoreJob(jobId -> createTestingJob(jobId, RestoreJobStatus.STAGE_READY, ConsistencyLevel.QUORUM));
        mockTopologyInRefresher(() -> {
            throw new IllegalStateException("Fails to load topology");
        });
        getThenComplete(context, TEST_PROGRESS_ROUTE,
                        asyncResult -> assertStatusAndErrorMessage(asyncResult, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                   "Fails to load topology"));
    }

    @Test
    void testRetrieveProgressFailsWhenFetchPolicyIsUnknown(VertxTestContext context)
    {
        mockLookupRestoreJob(jobId -> createTestingJob(jobId, RestoreJobStatus.STAGE_READY, ConsistencyLevel.QUORUM));
        getThenComplete(context, TEST_PROGRESS_ROUTE + "?fetch-policy=unknown_policy",
                        asyncResult -> assertStatusAndErrorMessage(asyncResult, HttpResponseStatus.BAD_REQUEST,
                                                                "No RestoreJobProgressFetchPolicy found for unknown_policy"));
    }

    @Test
    void testRetrieveProgressFailsWhenSliceCountIsNotSet(VertxTestContext context)
    {
        mockLookupRestoreJob(jobId -> createTestingJob(jobId, RestoreJobStatus.STAGE_READY, ConsistencyLevel.QUORUM)
                                      .unbuild()
                                      .sliceCount(null) // unset sliceCount
                                      .build());
        getThenComplete(context, TEST_PROGRESS_ROUTE + "?fetch-policy=first_failed",
                        asyncResult -> assertStatusAndErrorMessage(asyncResult, HttpResponseStatus.BAD_REQUEST,
                                                                   "Controller must set the sliceCount for Sidecar-managed restore job. " +
                                                                   "jobId=" + TEST_JOB_ID));
    }

    @Test
    void testRetrieveProgressUsingAll(VertxTestContext context)
    {
        restoreJobProgressSetupWithFailedRange();
        getThenComplete(context, TEST_PROGRESS_ROUTE + "?fetch-policy=all",
                        asyncResult -> {
                            int rangesRetrieved = 0;
                            RestoreJobProgressResponsePayload respBody = assertOKResponseAndExtractBody(asyncResult);
                            assertFailedProgressRespBody(respBody);
                            List<RestoreRangeJson> failedRanges = respBody.failedRanges();
                            assertThat(failedRanges).hasSize(1);
                            rangesRetrieved += failedRanges.size();
                            assertRange(failedRanges.get(0), 0, 10);
                            assertThat(respBody.succeededRanges()).isNull();
                            List<RestoreRangeJson> pendingRanges = respBody.pendingRanges();
                            assertThat(pendingRanges).hasSize(1);
                            rangesRetrieved += pendingRanges.size();
                            assertRange(pendingRanges.get(0), 10, 15);
                            assertThat(respBody.abortedRanges()).isNull();
                            // retrieving all 2 ranges back
                            assertThat(rangesRetrieved).isEqualTo(2);
                            assertThat(sidecarMetrics.server().restore().consistencyCheckTime.metric.getSnapshot().getValues()).hasSize(1);
                        });
    }

    @Test
    void testRetrieveProgressUsingAllFailedAndPending(VertxTestContext context)
    {
        restoreJobProgressSetupWithFailedRange();
        getThenComplete(context, TEST_PROGRESS_ROUTE + "?fetch-policy=all_failed_and_pending",
                        asyncResult -> {
                            int rangesRetrieved = 0;
                            RestoreJobProgressResponsePayload respBody = assertOKResponseAndExtractBody(asyncResult);
                            assertFailedProgressRespBody(respBody);
                            List<RestoreRangeJson> failedRanges = respBody.failedRanges();
                            assertThat(failedRanges).hasSize(1);
                            rangesRetrieved += failedRanges.size();
                            assertRange(failedRanges.get(0), 0, 10);
                            assertThat(respBody.succeededRanges()).isNull();
                            List<RestoreRangeJson> pendingRanges = respBody.pendingRanges();
                            assertThat(pendingRanges).hasSize(1);
                            rangesRetrieved += pendingRanges.size();
                            assertRange(pendingRanges.get(0), 10, 15);
                            assertThat(respBody.abortedRanges()).isNull();
                            // retrieving all 2 ranges back, while there are 3 ranges in total. One range is satisfied
                            assertThat(rangesRetrieved).isEqualTo(2);
                            assertThat(sidecarMetrics.server().restore().consistencyCheckTime.metric.getSnapshot().getValues()).hasSize(1);
                        });
    }

    @Test
    void testRetrieveProgressDefaultsToFirstFailed(VertxTestContext context)
    {
        // do not add the query parameter, first_failed should be assumed
        testFirstFailed(context, false);
    }

    @Test
    void testRetrieveProgressUsingFirstFailed(VertxTestContext context)
    {
        testFirstFailed(context, true);
    }

    void testFirstFailed(VertxTestContext context, boolean withQueryParam)
    {
        restoreJobProgressSetupWithFailedRange();
        getThenComplete(context, TEST_PROGRESS_ROUTE + (withQueryParam ? "?fetch-policy=first_failed" : ""),
                        asyncResult -> {
                            RestoreJobProgressResponsePayload respBody = assertOKResponseAndExtractBody(asyncResult);
                            assertFailedProgressRespBody(respBody);
                            List<RestoreRangeJson> failedRanges = respBody.failedRanges();
                            assertThat(failedRanges).hasSize(1);
                            assertRange(failedRanges.get(0), 0, 10);
                            // no ranges in other status are included in the response body when using FIRST_FAILED fetch policy
                            assertThat(respBody.succeededRanges()).isNull();
                            assertThat(respBody.pendingRanges()).isNull();
                            assertThat(respBody.abortedRanges()).isNull();
                        });
    }

    private void restoreJobProgressSetupWithFailedRange()
    {
        mockLookupRestoreJob(jobId -> createTestingJob(jobId, RestoreJobStatus.STAGE_READY, ConsistencyLevel.QUORUM)
                                      .unbuild().sliceCount(2L).build()); // the test creates 2 slices
        mockTopologyInRefresher(() -> generateTestTopology(3));
        Map<String, RestoreRangeStatus> failedStatus = new HashMap<>();
        failedStatus.put("instance-1", RestoreRangeStatus.FAILED);
        failedStatus.put("instance-2", RestoreRangeStatus.FAILED);
        RestoreRange failedRange = RestoreRangeTest.createTestRange(0, 10)
                                                   .unbuild().sliceId("slice-id1").replicaStatus(failedStatus).build();
        RestoreRange pendingRange = RestoreRangeTest.createTestRange(10, 15).unbuild().sliceId("slice-id2").build();
        List<RestoreRange> ranges = Arrays.asList(failedRange, pendingRange);
        mockFindAllRestoreRanges(jobId -> ranges);
    }

    // generate the artificial topology with fixed range length of 10 for each.
    // each range has 3 replicas
    private TokenRangeReplicasResponse generateTestTopology(int ranges)
    {
        String instance = "instance-";
        List<ReplicaInfo> writeReplicas = IntStream.range(0, ranges).boxed().map(i -> {
            int start = i * 10;
            int end = 10 + i * 10;
            // 3 replicas
            List<String> replicas = Arrays.asList(instance + i, instance + (i + 1) % ranges, instance + (i + 2) % ranges);
            Map<String, List<String>> replicasByDc = new HashMap<>();
            replicasByDc.put("dc1", replicas);
            return new ReplicaInfo(String.valueOf(start), String.valueOf(end), replicasByDc);
        }).collect(Collectors.toList());
        return new TokenRangeReplicasResponse(writeReplicas, Collections.emptyList(), Collections.emptyMap());
    }

    private void assertStatusAndErrorMessage(AsyncResult<HttpResponse<Buffer>> asyncResult, HttpResponseStatus status, String message)
    {
        HttpResponse<?> response = asyncResult.result();
        assertThat(response).isNotNull();
        assertThat(response.statusCode()).isEqualTo(status.code());
        assertThat(response.bodyAsJsonObject().getString("message")).isEqualTo(message);
    }

    private RestoreJobProgressResponsePayload assertOKResponseAndExtractBody(AsyncResult<HttpResponse<Buffer>> asyncResult)
    {
        HttpResponse<?> response = asyncResult.result();
        assertThat(response).isNotNull();
        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        return response.bodyAsJson(RestoreJobProgressResponsePayload.class);
    }

    private void assertRange(RestoreRangeJson range, long start, long end)
    {
        assertThat(range.startToken()).isEqualTo(BigInteger.valueOf(start));
        assertThat(range.endToken()).isEqualTo(BigInteger.valueOf(end));
    }

    private void assertFailedProgressRespBody(RestoreJobProgressResponsePayload respBody)
    {
        assertProgressRespBody(respBody, "One or more ranges have failed.", ConsistencyVerificationResult.FAILED);
    }

    private void assertPendingProgressRespBody(RestoreJobProgressResponsePayload respBody)
    {
        assertProgressRespBody(respBody, "One or more ranges are in progress. None of the ranges fail.", ConsistencyVerificationResult.PENDING);
    }

    private void assertProgressRespBody(RestoreJobProgressResponsePayload respBody, String message, ConsistencyVerificationResult status)
    {
        assertThat(respBody).isNotNull();
        assertThat(respBody.message()).startsWith(message);
        assertThat(respBody.status()).isEqualTo(status);
        assertThat(respBody.summary().jobId()).hasToString(TEST_JOB_ID);
    }
}
