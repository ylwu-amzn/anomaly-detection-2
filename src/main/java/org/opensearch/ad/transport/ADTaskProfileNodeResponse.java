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
import java.util.stream.Collectors;

import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.ad.cluster.ADVersionUtil;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import com.google.common.collect.ImmutableList;

public class ADTaskProfileNodeResponse extends BaseNodeResponse {
    private List<ADTaskProfile> adTaskProfiles;
    private Version remoteAdVersion;

    public ADTaskProfileNodeResponse(DiscoveryNode node, List<ADTaskProfile> adTaskProfile, Version remoteAdVersion) {
        super(node);
        this.adTaskProfiles = adTaskProfile;
        this.remoteAdVersion = remoteAdVersion;
    }

    public ADTaskProfileNodeResponse(StreamInput in, HashRing hashRing) throws IOException {
        super(in);
        String remoteNodeId = this.getNode().getId();
        String version = hashRing.getAdVersionString(remoteNodeId);
        Version remoteAdVersion = ADVersionUtil.fromString(version);
        if (in.readBoolean()) {
            if (remoteAdVersion.onOrBefore(Version.V_1_0_0)) {
                ADTaskProfile adTaskProfile = new ADTaskProfile(in);
                this.adTaskProfiles = ImmutableList.of(adTaskProfile);
            } else {
                this.adTaskProfiles = in.readList(ADTaskProfile::new);
            }
        } else {
            this.adTaskProfiles = null;
        }
    }

    public List<ADTaskProfile> getAdTaskProfiles() {
        return adTaskProfiles;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (remoteAdVersion.onOrBefore(Version.V_1_0_0)) {
            ADTaskProfile detectorTaskProfile = null;
            if (adTaskProfiles != null && adTaskProfiles.size() > 0) {
                List<ADTaskProfile> profiles = adTaskProfiles
                    .stream()
                    .filter(t -> t.getAdTask() != null && t.getAdTask().getParentTaskId() == null)
                    .collect(Collectors.toList());
                if (profiles.size() > 0) {
                    detectorTaskProfile = profiles.get(0);
                }
            }
            if (detectorTaskProfile != null) {
                out.writeBoolean(true);
                detectorTaskProfile.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        } else {
            if (adTaskProfiles != null && adTaskProfiles.size() > 0) {
                out.writeList(adTaskProfiles);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    public static ADTaskProfileNodeResponse readNodeResponse(StreamInput in, HashRing hashRing) throws IOException {
        return new ADTaskProfileNodeResponse(in, hashRing);
    }
}
