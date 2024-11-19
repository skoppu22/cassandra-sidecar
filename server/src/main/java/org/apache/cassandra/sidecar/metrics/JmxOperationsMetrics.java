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

package org.apache.cassandra.sidecar.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * Functionality to record metrics related to invocation of C* JMX operations
 */
public class JmxOperationsMetrics
{
    public static final String DOMAIN = INSTANCE_PREFIX + ".Cassandra.JmxOperations";

    protected final MetricRegistry metricRegistry;
    Map<String, NamedMetric<Timer>> operationMetrics = new HashMap<>();

    public JmxOperationsMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");
    }

    public void recordTimeTaken(String operationName, long durationNanos)
    {
        operationMetrics
        .computeIfAbsent(operationName,
                         k -> NamedMetric.builder(metricRegistry::timer).withDomain(DOMAIN).withName(operationName).build())
        .metric.update(durationNanos, TimeUnit.NANOSECONDS);
    }

    @VisibleForTesting
    public NamedMetric<Timer> getMetric(String operationName)
    {
        return operationMetrics.get(operationName);
    }
}
