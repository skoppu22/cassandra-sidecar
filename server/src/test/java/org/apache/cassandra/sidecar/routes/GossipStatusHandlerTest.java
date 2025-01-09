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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.response.GossipStatusResponse;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link GossipInfoHandler}
 */
@ExtendWith(VertxExtension.class)
public class GossipStatusHandlerTest extends JmxCommonTest
{
    private static final String testRoute = "/api/v1/cassandra/gossip/status";

    @Test
    void testGossipRunning(VertxTestContext context)
    {
        when(storageOperations.gossipStatus()).thenReturn(new GossipStatusResponse("RUNNING"));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context, response, "RUNNING")));
    }

    @Test
    void testGossipNotRunning(VertxTestContext context)
    {
        when(storageOperations.gossipStatus()).thenReturn(new GossipStatusResponse("NOT_RUNNING"));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context, response, "NOT_RUNNING")));
    }

    @Test
    void testWithInstanceId(VertxTestContext context)
    {
        when(storageOperations.gossipStatus()).thenReturn(new GossipStatusResponse("RUNNING"));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute + "?instanceId=200")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context, response, "RUNNING")));
    }

    @Test
    void testFailure(VertxTestContext context)
    {
        doThrow(new RuntimeException()).when(storageOperations).gossipStatus();

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_INTERNAL_SERVER_ERROR)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(INTERNAL_SERVER_ERROR.code());
                  context.completeNow();
              }));
    }

    @Test
    void testNullStorageOps(VertxTestContext context)
    {
        when(delegate.storageOperations()).thenReturn(null);

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_SERVICE_UNAVAILABLE)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
                  context.completeNow();
              }));
    }

    private void verifyValidResponse(VertxTestContext context, HttpResponse<Buffer> response, String expectedValue)
    {
        context.verify(() -> {
            JsonObject responseJson = response.bodyAsJsonObject();
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(responseJson.getString("gossipRunningStatus")).isEqualTo(expectedValue);
            context.completeNow();
        });
    }

}
