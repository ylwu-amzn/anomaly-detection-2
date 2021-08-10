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

import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.ad.cluster.ADVersionUtil;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADTaskProfileNodeRequest extends AbstractADBaseNodeRequest {
    private String detectorId;

    public ADTaskProfileNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readString();
    }

    public ADTaskProfileNodeRequest(ADTaskProfileRequest request, String adVersion) {
        this.detectorId = request.getDetectorId();
        setAdVersion(adVersion);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorId);
        if (!ADVersionUtil.fromString(getAdVersion()).onOrBefore(Version.V_1_0_0)) {
            out.writeString(getAdVersion());
        }
    }

    public String getDetectorId() {
        return detectorId;
    }

}
