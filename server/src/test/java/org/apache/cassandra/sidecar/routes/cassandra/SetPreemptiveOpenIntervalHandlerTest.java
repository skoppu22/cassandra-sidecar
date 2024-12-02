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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doNothing;

@ExtendWith(VertxExtension.class)
class SetPreemptiveOpenIntervalHandlerTest extends JmxCommonTest
{
    private static final String testRoute = "/api/v1/cassandra/sstable/preemptive-open-interval";
    @Test
    void testWithoutInstanceId(VertxTestContext context)
    {
        doNothing().when(storageOperations).setSSTablePreemptiveOpenIntervalInMB(anyInt());

        WebClient client = WebClient.create(vertx);
        client.put(server.actualPort(), "127.0.0.1", testRoute + "/50")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyResponse(context,
                                                                  response,
                                                                  "50")));
    }

    @Test
    void testWithInstanceId(VertxTestContext context)
    {
        doNothing().when(storageOperations).setSSTablePreemptiveOpenIntervalInMB(anyInt());

        WebClient client = WebClient.create(vertx);
        client.put(server.actualPort(), "127.0.0.1", testRoute + "/60?instanceId=200")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyResponse(context,
                                                                  response,
                                                                  "60")));
    }

    @Test
    void testFailure(VertxTestContext context)
    {
        doThrow(new RuntimeException()).when(storageOperations).setSSTablePreemptiveOpenIntervalInMB(50);

        WebClient client = WebClient.create(vertx);
        client.put(server.actualPort(), "127.0.0.1", testRoute + "/50")
              .expect(ResponsePredicate.SC_INTERNAL_SERVER_ERROR)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(INTERNAL_SERVER_ERROR.code());
                  context.completeNow();
              }));
    }

    private void verifyResponse(VertxTestContext context, HttpResponse<Buffer> response, String expectedValue)
    {
        context.verify(() -> {
            JsonObject responseJson = response.bodyAsJsonObject();
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(responseJson.getString("SSTablePreemptiveOpenIntervalInMB")).isEqualTo(expectedValue);
            context.completeNow();
        });
    }
}
