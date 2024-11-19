package org.apache.cassandra.sidecar.routes.cassandra.management;

import java.util.Collections;
import javax.inject.Singleton;

import com.google.inject.Inject;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Functionality to retrieve sstable's preemptive open interval value
 */
@Singleton
public class GetPreemptiveOpenIntervalHandler extends JmxOperationsHandler<Void>
{
    public static final String SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB = "SSTablePreemptiveOpenIntervalInMB";

    @Inject
    protected GetPreemptiveOpenIntervalHandler(InstanceMetadataFetcher metadataFetcher,
                                               ExecutorPools executorPools,
                                               CassandraInputValidator validator,
                                               SidecarMetrics sidecarMetrics)
    {
        super(metadataFetcher, executorPools, validator, sidecarMetrics);
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  Void request)
    {
        getSSTablePreemptiveOpenInterval(host, remoteAddress)
        .onSuccess(result -> context.json(Collections.singletonMap(SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB,
                                                                   result)))
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }
}
