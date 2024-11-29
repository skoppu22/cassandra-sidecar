package org.apache.cassandra.sidecar.routes.cassandra;

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(VertxExtension.class)
public class GetPreemptiveOpenIntervalHandlerIntegrationTest extends IntegrationTestBase
{
    private static final String testRoute = "/api/v1/cassandra/sstable/preemptive-open-interval";

    @CassandraIntegrationTest
    void testRetrieveConfigValue(CassandraTestContext context, VertxTestContext testContext)
    {
        client.get(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(testContext.succeeding(response -> verifyResponse(context, testContext, response)));
    }

    void verifyResponse(CassandraTestContext context, VertxTestContext testContext, HttpResponse<Buffer> response)
    {
        IUpgradeableInstance instance = context.cluster().getFirstRunningInstance();
        IInstanceConfig config = instance.config();

        testContext.verify(() -> {
            JsonObject responseJson = response.bodyAsJsonObject();
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(responseJson.getString(GetPreemptiveOpenIntervalHandler.SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB))
            .isEqualTo("50");
            testContext.completeNow();
        });
    }
}
