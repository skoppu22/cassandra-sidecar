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
        when(storageOperations.isGossipRunning()).thenReturn(new GossipStatusResponse(true));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context, response, true)));
    }

    @Test
    void testGossipNotRunning(VertxTestContext context)
    {
        when(storageOperations.isGossipRunning()).thenReturn(new GossipStatusResponse(false));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context, response, false)));
    }

    @Test
    void testWithInstanceId(VertxTestContext context)
    {
        when(storageOperations.isGossipRunning()).thenReturn(new GossipStatusResponse(true));

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute + "?instanceId=200")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyValidResponse(context, response, true)));
    }

    @Test
    void testFailure(VertxTestContext context)
    {
        doThrow(new RuntimeException()).when(storageOperations).isGossipRunning();

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

    private void verifyValidResponse(VertxTestContext context, HttpResponse<Buffer> response, boolean expectedValue)
    {
        context.verify(() -> {
            JsonObject responseJson = response.bodyAsJsonObject();
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(responseJson.getBoolean("gossipRunning")).isEqualTo(expectedValue);
            context.completeNow();
        });
    }

}
