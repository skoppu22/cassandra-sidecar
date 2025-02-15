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

package org.apache.cassandra.sidecar.routes.snapshots;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class ClearSnapshotHandlerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void deleteSnapshotFailsWhenKeyspaceDoesNotExist(VertxTestContext context) throws Exception
    {
        String testRoute = "/api/v1/keyspaces/non_existent/tables/testtable/snapshots/my-snapshot";
        assertNotFoundOnDeleteSnapshot(context, testRoute);
    }

    @CassandraIntegrationTest
    void deleteSnapshotFailsWhenTableDoesNotExist(VertxTestContext context)
    throws Exception
    {
        createTestKeyspace();
        createTestTableAndPopulate();

        String testRoute = "/api/v1/keyspaces/testkeyspace/tables/non_existent/snapshots/my-snapshot";
        assertNotFoundOnDeleteSnapshot(context, testRoute);
    }

    @CassandraIntegrationTest(numDataDirsPerInstance = 1)
        // Set to > 1 to fail test
    void testDeleteSnapshotEndpoint(VertxTestContext context)
    throws Exception
    {
        createTestKeyspace();
        QualifiedTableName tableName = createTestTableAndPopulate();

        WebClient client = mTLSClient();
        String snapshotName = "my-snapshot" + UUID.randomUUID();
        String testRoute = String.format("/api/v1/keyspaces/%s/tables/%s/snapshots/%s",
                                         tableName.maybeQuotedKeyspace(), tableName.maybeQuotedTableName(),
                                         snapshotName);

        // first create the snapshot
        client.put(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(
              createResponse ->
              context.verify(() -> {
                  assertThat(createResponse.statusCode()).isEqualTo(OK.code());
                  final List<Path> found =
                  findChildFile(sidecarTestContext, "127.0.0.1", tableName.keyspace(), snapshotName);
                  assertThat(found).isNotEmpty();

                  // snapshot directory exists inside data directory
                  assertThat(found).isNotEmpty();

                  // then delete the snapshot
                  client.delete(server.actualPort(), "127.0.0.1", testRoute)
                        .expect(ResponsePredicate.SC_OK)
                        .send(context.succeeding(
                        deleteResponse ->
                        context.verify(() ->
                                       {
                                           assertThat(deleteResponse.statusCode()).isEqualTo(OK.code());
                                           final List<Path> found2 =
                                           findChildFile(sidecarTestContext,
                                                         "127.0.0.1",
                                                         tableName.keyspace(),
                                                         snapshotName);
                                           assertThat(found2).isEmpty();
                                           context.completeNow();
                                       })));
              })));
        // wait until the test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }

    private QualifiedTableName createTestTableAndPopulate()
    {
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE %s (id text PRIMARY KEY, name text)" + WITH_COMPACTION_DISABLED + ";");
        Session session = maybeGetSession();

        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('1', 'Francisco');");
        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('2', 'Saranya');");
        session.execute("INSERT INTO " + tableName + " (id, name) VALUES ('3', 'Yifan');");
        return tableName;
    }

    private void assertNotFoundOnDeleteSnapshot(VertxTestContext context, String testRoute) throws Exception
    {
        WebClient client = mTLSClient();
        client.delete(server.actualPort(), "127.0.0.1", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeedingThenComplete());
        // wait until test completes
        assertThat(context.awaitCompletion(30, TimeUnit.SECONDS)).isTrue();
    }
}
