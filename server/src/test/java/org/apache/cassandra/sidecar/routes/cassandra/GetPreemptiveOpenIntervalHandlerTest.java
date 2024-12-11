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
import org.apache.cassandra.sidecar.common.response.GetPreemptiveOpenIntervalResponse;
import org.apache.cassandra.sidecar.common.server.DataStorageUnit;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class GetPreemptiveOpenIntervalHandlerTest extends JmxCommonTest
{
    private static final String testRoute = "/api/v1/cassandra/sstable/preemptive-open-interval";

    @Test
    void testWithoutInstanceId(VertxTestContext context)
    {
        when(storageOperations.getSSTablePreemptiveOpenInterval(DataStorageUnit.MEBIBYTES))
        .thenReturn(new GetPreemptiveOpenIntervalResponse(20));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context,
                                                                       response,
                                                                       "20")));
    }

    @Test
    void testWithInstanceId(VertxTestContext context)
    {
        when(storageOperations.getSSTablePreemptiveOpenInterval(DataStorageUnit.MEBIBYTES))
        .thenReturn(new GetPreemptiveOpenIntervalResponse(10));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute + "?instanceId=200")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context,
                                                                       response,
                                                                       "10")));
    }

    @Test
    void testWithUnitParam(VertxTestContext context)
    {
        when(storageOperations.getSSTablePreemptiveOpenInterval(DataStorageUnit.MEBIBYTES))
        .thenReturn(new GetPreemptiveOpenIntervalResponse(30));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute + "?unit=MiB")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context,
                                                                       response,
                                                                       "30")));
    }

    @Test
    void testWithUnsupportedUnitParam(VertxTestContext context)
    {
        when(storageOperations.getSSTablePreemptiveOpenInterval(DataStorageUnit.GIBIBYTES))
        .thenReturn(new GetPreemptiveOpenIntervalResponse(40));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute + "?unit=GiB")
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(BAD_REQUEST.code());
                  assertThat(response.bodyAsJsonObject().getString("message"))
                  .isEqualTo("Invalid value provided for unit GiB, expected MiB");
                  context.completeNow();
              })));
    }

    @Test
    void testWithUnitAndInstanceParam(VertxTestContext context)
    {
        when(storageOperations.getSSTablePreemptiveOpenInterval(DataStorageUnit.MEBIBYTES))
        .thenReturn(new GetPreemptiveOpenIntervalResponse(50));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute + "?unit=MiB&instanceId=200")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context,
                                                                       response,
                                                                       "50")));
    }

    @Test
    void testFailure(VertxTestContext context)
    {
        doThrow(new RuntimeException()).when(storageOperations)
                                       .getSSTablePreemptiveOpenInterval(DataStorageUnit.MEBIBYTES);

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
            assertThat(responseJson.getString("SSTablePreemptiveOpenInterval")).isEqualTo(expectedValue);
            context.completeNow();
        });
    }
}
