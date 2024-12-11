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
 * Response for GET preemptive open interval API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetPreemptiveOpenIntervalResponse
{
    private final int sstablePreemptiveOpenInterval;

    /**
     * Constructs a response with preemptive open interval value
     * @param sstablePreemptiveOpenInterval value of preemptive open interval
     */
    public GetPreemptiveOpenIntervalResponse(@JsonProperty("SSTablePreemptiveOpenInterval") int sstablePreemptiveOpenInterval)
    {
        this.sstablePreemptiveOpenInterval = sstablePreemptiveOpenInterval;
    }

    @JsonProperty("SSTablePreemptiveOpenInterval")
    public int sstablePreemptiveOpenInterval()
    {
        return sstablePreemptiveOpenInterval;
    }
}
