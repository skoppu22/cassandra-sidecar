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

package org.apache.cassandra.sidecar.cluster.instance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.utils.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL_AND_JMX;

/**
 * Local implementation of InstanceMetadata.
 */
public class InstanceMetadataImpl implements InstanceMetadata
{
    private final int id;
    private final String host;
    private final int port;
    private final List<String> dataDirs;
    private final String stagingDir;
    private final String cdcDir;
    @Nullable
    private final CassandraAdapterDelegate delegate;
    private final InstanceMetrics metrics;

    protected InstanceMetadataImpl(Builder builder)
    {
        id = builder.id;
        host = builder.host;
        port = builder.port;
        dataDirs = builder.dataDirs.stream()
                                   .map(FileUtils::maybeResolveHomeDirectory)
                                   .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
        stagingDir = FileUtils.maybeResolveHomeDirectory(builder.stagingDir);
        cdcDir = FileUtils.maybeResolveHomeDirectory(builder.cdcDir);
        delegate = builder.delegate;
        metrics = builder.metrics;
    }

    @Override
    public int id()
    {
        return id;
    }

    @Override
    public String host()
    {
        return host;
    }

    @Override
    public int port()
    {
        return port;
    }

    @Override
    public List<String> dataDirs()
    {
        return dataDirs;
    }

    @Override
    public String stagingDir()
    {
        return stagingDir;
    }

    @Override
    public String cdcDir()
    {
        return cdcDir;
    }

    @Override
    @NotNull
    public CassandraAdapterDelegate delegate() throws CassandraUnavailableException
    {
        if (delegate == null)
        {
            throw new CassandraUnavailableException(CQL_AND_JMX, "CassandraAdapterDelegate is null");
        }
        return delegate;
    }

    @Override
    @NotNull
    public InstanceMetrics metrics()
    {
        return metrics;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String toString()
    {
        return "InstanceMetadataImpl{" +
               "id=" + id +
               ", host='" + host + '\'' +
               ", port=" + port +
               '}';
    }

    /**
     * {@code InstanceMetadataImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, InstanceMetadataImpl>
    {
        protected Integer id;
        protected String host;
        protected int port;
        protected List<String> dataDirs;
        protected String stagingDir;
        protected String cdcDir;
        protected CassandraAdapterDelegate delegate;
        protected MetricRegistry metricRegistry;
        protected InstanceMetrics metrics;

        protected Builder()
        {
        }

        protected Builder(InstanceMetadataImpl instanceMetadata)
        {
            id = instanceMetadata.id;
            host = instanceMetadata.host;
            port = instanceMetadata.port;
            dataDirs = new ArrayList<>(instanceMetadata.dataDirs);
            stagingDir = instanceMetadata.stagingDir;
            cdcDir = instanceMetadata.cdcDir;
            delegate = instanceMetadata.delegate;
            metrics = instanceMetadata.metrics;
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code id} and returns a reference to this Builder enabling method chaining.
         *
         * @param id the {@code id} to set
         * @return a reference to this Builder
         */
        public Builder id(int id)
        {
            return update(b -> b.id = id);
        }

        /**
         * Sets the {@code host} and returns a reference to this Builder enabling method chaining.
         *
         * @param host the {@code host} to set
         * @return a reference to this Builder
         */
        public Builder host(String host)
        {
            return update(b -> b.host = host);
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            return update(b -> b.port = port);
        }

        /**
         * Sets the {@code dataDirs} and returns a reference to this Builder enabling method chaining.
         *
         * @param dataDirs the {@code dataDirs} to set
         * @return a reference to this Builder
         */
        public Builder dataDirs(List<String> dataDirs)
        {
            return update(b -> b.dataDirs = dataDirs);
        }

        /**
         * Sets the {@code stagingDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param stagingDir the {@code stagingDir} to set
         * @return a reference to this Builder
         */
        public Builder stagingDir(String stagingDir)
        {
            return update(b -> b.stagingDir = stagingDir);
        }

        /**
         * Sets the {@code cdcDir} and returns a reference to this Builder enabling method chaining.
         *
         * @param cdcDir the {@code cdcDir} to set
         * @return a reference to this Builder
         */
        public Builder cdcDir(String cdcDir)
        {
            return update(b -> b.cdcDir = cdcDir);
        }

        /**
         * Sets the {@code delegate} and returns a reference to this Builder enabling method chaining.
         *
         * @param delegate the {@code delegate} to set
         * @return a reference to this Builder
         */
        public Builder delegate(CassandraAdapterDelegate delegate)
        {
            return update(b -> b.delegate = delegate);
        }

        /**
         * Sets the {@code metricRegistry} and returns a reference to this Builder enabling method chaining.
         *
         * @param metricRegistry instance specific metric registry
         * @return a reference to this Builder
         */
        public Builder metricRegistry(MetricRegistry metricRegistry)
        {
            return update(b -> b.metricRegistry = metricRegistry);
        }

        /**
         * Returns a {@code InstanceMetadataImpl} built from the parameters previously set.
         *
         * @return a {@code InstanceMetadataImpl} built with parameters of this {@code InstanceMetadataImpl.Builder}
         */
        @Override
        public InstanceMetadataImpl build()
        {
            Objects.requireNonNull(id);
            Objects.requireNonNull(metricRegistry);

            metrics = new InstanceMetricsImpl(metricRegistry);

            return new InstanceMetadataImpl(this);
        }
    }
}
