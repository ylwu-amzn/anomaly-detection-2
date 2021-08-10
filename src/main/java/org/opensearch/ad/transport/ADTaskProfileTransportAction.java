/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class ADTaskProfileTransportAction extends
    TransportNodesAction<ADTaskProfileRequest, ADTaskProfileResponse, ADTaskProfileNodeRequest, ADTaskProfileNodeResponse> {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private ADTaskManager adTaskManager;
    private HashRing hashRing;
    private Version remoteAdVersion;

    @Inject
    public ADTaskProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADTaskManager adTaskManager,
        HashRing hashRing
    ) {
        super(
            ADTaskProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ADTaskProfileRequest::new,
            ADTaskProfileNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ADTaskProfileNodeResponse.class
        );
        this.adTaskManager = adTaskManager;
        this.hashRing = hashRing;
    }

    @Override
    protected ADTaskProfileResponse newResponse(
        ADTaskProfileRequest request,
        List<ADTaskProfileNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        logger.info("11112222333344441111222233334444 new response: {}, hashRing: {}", responses.size(), hashRing);
        return new ADTaskProfileResponse(clusterService.getClusterName(), responses, failures, hashRing);
    }

    @Override
    protected ADTaskProfileNodeRequest newNodeRequest(ADTaskProfileRequest request) {
        return new ADTaskProfileNodeRequest(request);
    }

    @Override
    protected ADTaskProfileNodeResponse newNodeResponse(StreamInput in) throws IOException {

        return new ADTaskProfileNodeResponse(in, hashRing);
    }

    @Override
    protected ADTaskProfileNodeResponse nodeOperation(ADTaskProfileNodeRequest request) {
//        if (!hashRing.hasSameAdVersionWithLocalNode(request.getParentTask().getNodeId())) {
//            throw new ADVersionConflictException("Can't support get task profile among different AD versions");
//        }
        String remoteNodeId = request.getParentTask().getNodeId();
        this.remoteAdVersion = hashRing.getAdVersionOfNode(remoteNodeId);
        logger.info("111111111111111111111111111111111111111111111111111111111111 remote ad version: {}, remoteNodeId: {}", remoteAdVersion, remoteNodeId);
        List<ADTaskProfile> adTaskProfile = adTaskManager.getLocalADTaskProfilesByDetectorId(request.getDetectorId());
        return new ADTaskProfileNodeResponse(clusterService.localNode(), adTaskProfile, remoteAdVersion);
    }
}
