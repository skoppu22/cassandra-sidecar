package org.apache.cassandra.sidecar.routes.cassandra;

import java.util.Collections;
import javax.inject.Singleton;

import com.google.inject.Inject;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.metrics.JmxOperationsMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Functionality to retrieve sstable's preemptive open interval value
 */
@Singleton
public class GetPreemptiveOpenIntervalHandler extends AbstractHandler<Void>
{
    public static final String SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB = "SSTablePreemptiveOpenIntervalInMB";
    private final JmxOperationsMetrics jmxOperationsMetrics;

    @Inject
    protected GetPreemptiveOpenIntervalHandler(InstanceMetadataFetcher metadataFetcher,
                                               ExecutorPools executorPools,
                                               CassandraInputValidator validator,
                                               SidecarMetrics sidecarMetrics)
    {
        super(metadataFetcher, executorPools, validator);
        this.jmxOperationsMetrics = sidecarMetrics.server().jmxOperationsMetrics();
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

    protected Future<Integer> getSSTablePreemptiveOpenInterval(String host, SocketAddress remoteAddress)
    {
        long startTime = System.nanoTime();
        StorageOperations storageOperations = getStorageOperations(host);
        logger.debug("Retrieving SSTable's preemptiveOpenInterval, remoteAddress={}, instance={}",
                     remoteAddress, host);

        return executorPools.service()
                            .executeBlocking(storageOperations::getSSTablePreemptiveOpenIntervalInMB)
                            .onComplete(ar -> updateJmxMetric(ar, jmxOperationsMetrics, "getSSTablePreemptiveOpenInterval", startTime));
    }
}
