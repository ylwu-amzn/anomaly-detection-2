package org.opensearch.ad.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public abstract class AbstractADTransportNodesAction extends TransportNodesAction {

    protected AbstractADTransportNodesAction(String actionName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, Writeable.Reader request, Writeable.Reader nodeRequest, String nodeExecutor, String finalExecutor, Class aClass) {
        super(actionName, threadPool, clusterService, transportService, actionFilters, request, nodeRequest, nodeExecutor, finalExecutor, aClass);
    }

    protected AbstractADTransportNodesAction(String actionName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, ActionFilters actionFilters, Writeable.Reader request, Writeable.Reader nodeRequest, String nodeExecutor, Class aClass) {
        super(actionName, threadPool, clusterService, transportService, actionFilters, request, nodeRequest, nodeExecutor, aClass);
    }

    public abstract boolean isCompatible();
}
