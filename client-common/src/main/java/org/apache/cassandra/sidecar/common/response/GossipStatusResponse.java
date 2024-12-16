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

package org.apache.cassandra.sidecar.common.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response for GET gossip status API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GossipStatusResponse
{
    private final boolean gossipRunning;

    /**
     * Constructs a response with gossip status
     * @param gossipRunning running status of gossip
     */
    public GossipStatusResponse(@JsonProperty("gossipRunning") boolean gossipRunning)
    {
        this.gossipRunning = gossipRunning;
    }

    @JsonProperty("gossipRunning")
    public boolean gossipRunning()
    {
        return gossipRunning;
    }
}
