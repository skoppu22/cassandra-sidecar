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

package org.apache.cassandra.sidecar.utils;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

import org.apache.cassandra.sidecar.common.response.TimeSkewResponse;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;

/**
 * Determine the time skew info for clients' queries.
 */
public class TimeSkewInfo
{
    private final TimeProvider timeProvider;
    private final ServiceConfiguration configuration;

    @Inject
    public TimeSkewInfo(TimeProvider timeProvider, ServiceConfiguration configuration)
    {
        this.timeProvider = timeProvider;
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    public TimeSkewResponse timeSkewResponse()
    {
        int allowableTimeSkewInMinutes = (int) configuration.allowableTimeSkew().to(TimeUnit.MINUTES);
        return new TimeSkewResponse(timeProvider.currentTimeMillis(), allowableTimeSkewInMinutes);
    }
}
