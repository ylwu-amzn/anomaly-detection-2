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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.ADVersionConflictException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADTaskProfileNodeRequest extends AbstractADBaseNodeRequest {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private HashRing hashRing;
    private String detectorId;

    public ADTaskProfileNodeRequest(StreamInput in) throws IOException {
        super(in);
        if (!isCompatible()) {
            throw new ADVersionConflictException("Can't read ADTaskProfileNodeRequest of old AD version");
        }
        this.detectorId = in.readString();
    }

    @Override
    public boolean isCompatible() {
        String nodeId = this.getParentTask().getNodeId();
        logger.info("--------------- yyyyyy, remote node id  {}", nodeId);
        return hashRing.hasSameAdVersion(nodeId);
    }

    public ADTaskProfileNodeRequest(ADTaskProfileRequest request) {
        this.detectorId = request.getDetectorId();
        this.hashRing = request.getHashRing();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorId);
    }

    public String getDetectorId() {
        return detectorId;
    }

}
