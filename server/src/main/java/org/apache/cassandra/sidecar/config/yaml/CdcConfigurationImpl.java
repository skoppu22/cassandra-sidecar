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
package org.apache.cassandra.sidecar.config.yaml;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.CdcConfiguration;

/**
 * Encapsulate configuration values for CDC
 */
public class CdcConfigurationImpl implements CdcConfiguration
{
    public static final String SEGMENT_HARD_LINK_CACHE_EXPIRY_IN_SECS_PROPERTY = "segment_hardlink_cache_expiry_in_secs";
    public static final long DEFAULT_SEGMENT_HARD_LINK_CACHE_EXPIRY_IN_SECS = 300;

    @JsonProperty(value = SEGMENT_HARD_LINK_CACHE_EXPIRY_IN_SECS_PROPERTY, defaultValue = DEFAULT_SEGMENT_HARD_LINK_CACHE_EXPIRY_IN_SECS + "")
    protected final long segmentHardLinkCacheExpiryInSecs;

    public CdcConfigurationImpl()
    {
        this.segmentHardLinkCacheExpiryInSecs = DEFAULT_SEGMENT_HARD_LINK_CACHE_EXPIRY_IN_SECS;
    }

    public CdcConfigurationImpl(long segmentHardLinkCacheExpiryInSecs)
    {
        this.segmentHardLinkCacheExpiryInSecs = segmentHardLinkCacheExpiryInSecs;
    }

    @Override
    @JsonProperty(value = SEGMENT_HARD_LINK_CACHE_EXPIRY_IN_SECS_PROPERTY)
    public long segmentHardlinkCacheExpiryInSecs()
    {
        return segmentHardLinkCacheExpiryInSecs;
    }
}
