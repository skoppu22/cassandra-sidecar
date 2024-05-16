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

package org.apache.cassandra.sidecar.routes.tokenrange;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.sidecar.testing.BootstrapBBUtils;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

/**
 * Multi-DC Cluster expansion scenarios integration tests for token range replica mapping endpoint with the in-jvm
 * dtest framework.
 *
 * Note: Some related test classes are broken down to have a single test case to parallelize test execution and
 * therefore limit the instance size required to run the tests from CircleCI as the in-jvm-dtests tests are memory bound
 */
@Tag("heavy")
@ExtendWith(VertxExtension.class)
public class JoiningTestMultiDC extends JoiningBaseTest
{
    @CassandraIntegrationTest(
    nodesPerDc = 3, newNodesPerDc = 3, numDcs = 2, network = true, buildCluster = false)
    void retrieveMappingsDoubleClusterSizeMultiDC(VertxTestContext context,
                                                  ConfigurableCassandraTestContext cassandraTestContext)
    throws Exception
    {
        BBHelperDoubleClusterMultiDC.reset();
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperDoubleClusterMultiDC::install, cassandraTestContext);

        runJoiningTestScenario(context,
                               BBHelperDoubleClusterMultiDC.transientStateStart,
                               BBHelperDoubleClusterMultiDC.transientStateEnd,
                               cluster,
                               generateExpectedRanges(),
                               generateExpectedRangeDoubleClusterSizeMultiDC(),
                               true);
    }

    /**
     * Generates expected token range and replica mappings specific to the test case involving a 6 node cluster
     * across 2 DCs with the 6 nodes joining the cluster (3 per DC)
     * <p>
     * Expected ranges are generated by adding RF replicas per range in increasing order. The replica-sets in subsequent
     * ranges cascade with the next range excluding the first replica, and including the next replica from the nodes.
     * eg.
     * Range 1 - A, B, C
     * Range 2 - B, C, D
     * <p>
     * In a multi-DC scenario, a single range will have nodes from both DCs. The replicas are grouped by DC here
     * to allow per-DC validation as returned from the sidecar endpoint.
     * <p>
     * We generate the expected ranges by using
     * 1) the initial token allocations to nodes (prior to adding nodes) shown under "Initial Ranges"
     * (in the comment block below),
     * 2)the "pending node ranges" and
     * 3) the final token allocations per node.
     * <p>
     * Step 1: Prepare ranges starting from partitioner min-token, ending at partitioner max-token using (3) above
     * Step 2: Create the cascading list of replica-sets based on the RF (3) for each range using the initial node list
     * Step 3: Add replicas to ranges based on (1) and (2) above
     */
    private Map<String, Map<Range<BigInteger>, List<String>>> generateExpectedRangeDoubleClusterSizeMultiDC()
    {
        /*
         * Initial Ranges:
         * [-9223372036854775808", "-4611686018427387907"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-4611686018427387907", "-4611686018427387906"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-4611686018427387906", "-7"]:["127.0.0.3","127.0.0.5","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-7", "-6"]:["127.0.0.5","127.0.0.3","127.0.0.1",
         *                                                  "127.0.0.6","127.0.0.2","127.0.0.4"]
         * [-6", "4611686018427387893"]:["127.0.0.5","127.0.0.3","127.0.0.1", "127.0.0.6",
         *                                                  "127.0.0.2","127.0.0.4"]
         * [4611686018427387893", "4611686018427387894"]:["127.0.0.3","127.0.0.5","127.0.0.1", "127.0.0.6","127.0.0.2",
         *                                                  "127.0.0.4"]
         * ["4611686018427387894"", "9223372036854775807"]:["127.0.0.3","127.0.0.5","127.0.0.1", "127.0.0.6",
         *                                                  "127.0.0.2","127.0.0.4:]
         *
         *  New Node tokens:
         * 127.0.0.7 at token -2305843009213693956
         * 127.0.0.8 at token -2305843009213693955
         * 127.0.0.9 at token 2305843009213693944
         * 127.0.0.10 at token 2305843009213693945
         * 127.0.0.11 at token 6917529027641081844
         * 127.0.0.12 at token 6917529027641081845
         *
         *
         * Pending Ranges:
         * [-6, 2305843009213693944]=[127.0.0.9:62801]
         * [-6, 2305843009213693945]=[127.0.0.10:62802]
         * [-6, 4611686018427387893]=[127.0.0.12:62804, 127.0.0.7:62799, 127.0.0.8:62800, 127.0.0.11:62803]
         * [4611686018427387894, -4611686018427387907]=[127.0.0.7:62799, 127.0.0.8:62800, 127.0.0.9:62801,
         * 127.0.0.10:62802] (wrap-around)
         * [-4611686018427387906, -2305843009213693956]=[127.0.0.7:62799]
         * [-4611686018427387907, -4611686018427387906]=[127.0.0.7:62799, 127.0.0.8:62800, 127.0.0.9:62801,
         * 127.0.0.10:62802, 127.0.0.11:62803]
         * [-4611686018427387906, -7]=[127.0.0.12:62804, 127.0.0.9:62801, 127.0.0.10:62802, 127.0.0.11:62803]
         * [-4611686018427387906, -2305843009213693955]=[127.0.0.8:62800]
         * [4611686018427387894, 6917529027641081844]=[127.0.0.11:62803]
         * [4611686018427387894, 6917529027641081845]=[127.0.0.12:62804]
         * [4611686018427387893, 4611686018427387894]=[127.0.0.12:62804, 127.0.0.7:62799, 127.0.0.8:62800,
         * 127.0.0.9:62801, 127.0.0.11:62803]
         *
         */

        List<Range<BigInteger>> expectedRanges = generateExpectedRanges();
        Map<Range<BigInteger>, List<String>> dc1Mapping = new HashMap<>();
        Map<Range<BigInteger>, List<String>> dc2Mapping = new HashMap<>();

        dc1Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(0), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6", "127.0.0.8",
                                                            "127.0.0.10"));

        dc1Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.1", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(1), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6", "127.0.0.8",
                                                            "127.0.0.10"));


        dc1Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.3", "127.0.0.5", "127.0.0.1", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(2), Arrays.asList("127.0.0.2", "127.0.0.4", "127.0.0.6", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));


        dc1Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.3", "127.0.0.9",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(3), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                            "127.0.0.8", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.5", "127.0.0.1", "127.0.0.3", "127.0.0.9",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(4), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                            "127.0.0.12"));
        dc1Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(5), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.10",
                                                            "127.0.0.12"));


        dc1Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(6), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(7), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.8",
                                                            "127.0.0.10", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.7", "127.0.0.11", "127.0.0.1", "127.0.0.3",
                                                            "127.0.0.5"));
        dc2Mapping.put(expectedRanges.get(8), Arrays.asList("127.0.0.6", "127.0.0.2", "127.0.0.4", "127.0.0.8",
                                                            "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                            "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(9), Arrays.asList("127.0.0.8", "127.0.0.12", "127.0.0.2", "127.0.0.6",
                                                            "127.0.0.4"));

        dc1Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                             "127.0.0.9", "127.0.0.11"));
        dc2Mapping.put(expectedRanges.get(10), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                             "127.0.0.8", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                             "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(11), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                             "127.0.0.8", "127.0.0.12"));

        dc1Mapping.put(expectedRanges.get(12), Arrays.asList("127.0.0.1", "127.0.0.3", "127.0.0.5", "127.0.0.7",
                                                             "127.0.0.9"));
        dc2Mapping.put(expectedRanges.get(12), Arrays.asList("127.0.0.4", "127.0.0.6", "127.0.0.2", "127.0.0.10",
                                                             "127.0.0.8"));

        return new HashMap<String, Map<Range<BigInteger>, List<String>>>()
        {
            {
                put("datacenter1", dc1Mapping);
                put("datacenter2", dc2Mapping);
            }
        };
    }

    /**
     * ByteBuddy helper for multiple joining nodes
     */
    public static class BBHelperDoubleClusterMultiDC
    {
        static CountDownLatch transientStateStart = new CountDownLatch(6);
        static CountDownLatch transientStateEnd = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves doubling the size of a 6 node cluster (3 per DC)
            // We intercept the bootstrap of nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                BootstrapBBUtils.installSetBoostrapStateIntercepter(cl, BBHelperDoubleClusterMultiDC.class);
            }
        }

        public static void setBootstrapState(SystemKeyspace.BootstrapState state, @SuperCall Callable<Void> orig) throws Exception
        {
            if (state == SystemKeyspace.BootstrapState.COMPLETED)
            {
                // trigger bootstrap start and wait until bootstrap is ready from test
                transientStateStart.countDown();
                awaitLatchOrTimeout(transientStateEnd, 2, TimeUnit.MINUTES, "transientStateEnd");
            }
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(6);
            transientStateEnd = new CountDownLatch(6);
        }
    }
}
