/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.sidecar.common.request;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.utils.HttpRange;

/**
 * Represents a request to stream CDC segments(commit logs) on an instance.
 */
public class StreamCdcSegmentRequest extends Request
{
    private final HttpRange range;

    public StreamCdcSegmentRequest(String segment, HttpRange range)
    {
        super(requestURI(segment));
        this.range = range;
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.GET;
    }

    @Override
    protected HttpRange range()
    {
        return range;
    }

    private static String requestURI(String segment)
    {
        return ApiEndpointsV1.STREAM_CDC_SEGMENTS_ROUTE.replaceAll(ApiEndpointsV1.SEGMENT_PATH_PARAM, segment);
    }
}
