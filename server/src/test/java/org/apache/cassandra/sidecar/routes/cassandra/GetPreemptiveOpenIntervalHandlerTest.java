package org.apache.cassandra.sidecar.routes.cassandra;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;

@ExtendWith(VertxExtension.class)
class GetPreemptiveOpenIntervalHandlerTest extends JmxCommonTest
{
    private static final String testRoute = "/api/v1/cassandra/sstable/preemptive-open-interval";

    @Test
    void testWithoutInstanceId(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyResponse(context,
                                                                  response,
                                                                  GetPreemptiveOpenIntervalHandler.SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB,
                                                                  "5")));
    }

    @Test
    void testWithInstanceId(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute + "?instanceId=200")
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> verifyResponse(context,
                                                                  response,
                                                                  GetPreemptiveOpenIntervalHandler.SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB,
                                                                  "5")));
    }

    @Test
    void testFailure(VertxTestContext context)
    {
        doThrow(new RuntimeException()).when(storageOperations).getSSTablePreemptiveOpenIntervalInMB();

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_INTERNAL_SERVER_ERROR)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(INTERNAL_SERVER_ERROR.code());
                  context.completeNow();
              }));
    }
}
