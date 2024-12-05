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

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test GET preemptive-open-interval endpoint with C* container
 */
@ExtendWith(VertxExtension.class)
public class GetPreemptiveOpenIntervalHandlerIntegrationTest extends IntegrationTestBase
{
    private static final String testRoute = "/api/v1/cassandra/sstable/preemptive-open-interval";

    @CassandraIntegrationTest
    void testDefaultValue(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(testContext.succeeding(response -> verifyValidResponse(testContext, response, 50)));
    }

    @CassandraIntegrationTest(yamlProps = "sstable_preemptive_open_interval_in_mb=60")
    void testPreemptiveOpenInterval60(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(testContext.succeeding(response -> verifyValidResponse(testContext, response, 60)));
    }

    @CassandraIntegrationTest(yamlProps = "sstable_preemptive_open_interval_in_mb=70")
    void testPreemptiveOpenInterval70(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(testContext.succeeding(response -> verifyValidResponse(testContext, response, 70)));
    }

    @CassandraIntegrationTest(yamlProps = "sstable_preemptive_open_interval_in_mb=-1")
    void testPreemptiveOpenIntervalNegative(VertxTestContext testContext)
    {
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(testContext.succeeding(response -> verifyValidResponse(testContext, response, -1)));
    }

    void verifyValidResponse(VertxTestContext testContext, HttpResponse<Buffer> response, int expectedValue)
    {
        testContext.verify(() -> {
            JsonObject responseJson = response.bodyAsJsonObject();
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(responseJson.getInteger("SSTablePreemptiveOpenIntervalInMB"))
            .isEqualTo(expectedValue);
            testContext.completeNow();
        });
    }
}
