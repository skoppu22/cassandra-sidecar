package org.apache.cassandra.sidecar.routes.cassandra.management;

import javax.inject.Singleton;

import com.google.inject.Inject;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.net.SocketAddress;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.metrics.JmxOperationsMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;

@Singleton
public abstract class JmxOperationsHandler<T> extends AbstractHandler<T>
{
    JmxOperationsMetrics jmxOperationsMetrics;

    @Inject
    protected JmxOperationsHandler(InstanceMetadataFetcher metadataFetcher,
                                   ExecutorPools executorPools,
                                   CassandraInputValidator validator,
                                   SidecarMetrics sidecarMetrics)
    {
        super(metadataFetcher, executorPools, validator);
        this.jmxOperationsMetrics = sidecarMetrics.server().jmxOperationsMetrics();
    }

    protected StorageOperations getStorageOperations(String host)
    {
        CassandraAdapterDelegate delegate = this.metadataFetcher.delegate(host);
        StorageOperations storageOperations = delegate == null ? null : delegate.storageOperations();
        if (storageOperations == null)
        {
            throw cassandraServiceUnavailable();
        }

        return storageOperations;
    }

    protected <V> void updateMetric(AsyncResult<V> result, String operationName, long startTime)
    {
        if (result.succeeded())
        {
            jmxOperationsMetrics.recordTimeTaken(operationName + "Succeeded",
                                                 System.nanoTime() - startTime);
        }
        else
        {
            jmxOperationsMetrics.recordTimeTaken(operationName + "Failed",
                                                 System.nanoTime() - startTime);
        }
    }

    protected Future<Integer> getSSTablePreemptiveOpenInterval(String host, SocketAddress remoteAddress)
    {
        long startTime = System.nanoTime();
        StorageOperations storageOperations = getStorageOperations(host);
        logger.debug("Retrieving SSTable's preemptiveOpenInterval, remoteAddress={}, instance={}",
                     remoteAddress, host);

        return executorPools.service()
                            .executeBlocking(storageOperations::getSSTablePreemptiveOpenIntervalInMB)
                            .onComplete(ar -> updateMetric(ar, "getSSTablePreemptiveOpenInterval", startTime));
    }
}
