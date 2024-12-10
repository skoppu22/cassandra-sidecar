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

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.HealthResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test GET gossip health endpoint with C* container
 */
@ExtendWith(VertxExtension.class)
public class GossipHealthHandlerIntegrationTest extends IntegrationTestBase
{
    private static final String testRoute = "/api/v1/cassandra/gossip/__health";

    private Future<HealthResponse> getGossipHealth()
    {
        return client.get(server.actualPort(), "127.0.0.1", testRoute)
                     .expect(ResponsePredicate.SC_OK)
                     .send()
                     .compose(response -> {
                         assertThat(response.statusCode()).isEqualTo(OK.code());
                         return Future.succeededFuture(response.bodyAsJson(HealthResponse.class));
                     });
    }

    @CassandraIntegrationTest()
    void testGossipHealth(CassandraTestContext context, VertxTestContext testContext)
    {
        int disableGossip = context.cluster().getFirstRunningInstance().nodetool("disablegossip");
        assertThat(disableGossip).isEqualTo(0);

        getGossipHealth()
        .compose(response -> {
            assertThat(response.status()).isEqualTo("NOT_OK");
            assertThat(context.cluster().getFirstRunningInstance().nodetool("enablegossip"))
            .isEqualTo(0);
            return getGossipHealth();
        })
        .compose(response -> {
            assertThat(response.status()).isEqualTo("OK");
            return Future.succeededFuture();
        })
        .onSuccess(response -> testContext.completeNow())
        .onFailure(testContext::failNow);
    }
}
