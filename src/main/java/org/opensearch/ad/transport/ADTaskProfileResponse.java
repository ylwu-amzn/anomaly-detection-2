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
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADTaskProfileResponse extends BaseNodesResponse<ADTaskProfileNodeResponse> {

    private final Logger logger = LogManager.getLogger(this.getClass());
    private HashRing hashRing;

    public ADTaskProfileResponse(StreamInput in, HashRing hashRing) throws IOException {
        super(new ClusterName(in), in.readList(input -> ADTaskProfileNodeResponse.readNodeResponse(input, hashRing)), in.readList(FailedNodeException::new));
        logger.info("0000000000000000000000000000000 ADTaskProfileResponse read from StreamInput");
    }

    public ADTaskProfileResponse(ClusterName clusterName, List<ADTaskProfileNodeResponse> nodes,
                                 List<FailedNodeException> failures, HashRing hashRing) {
        super(clusterName, nodes, failures);
        this.hashRing = hashRing;
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<ADTaskProfileNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<ADTaskProfileNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        logger.info("0000000000000000000000000000000 HashRing is  " + hashRing);
        return in.readList(streamInput -> ADTaskProfileNodeResponse.readNodeResponse(in, hashRing));
    }

}
