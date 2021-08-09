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

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADTaskProfileNodeResponse extends BaseNodeResponse {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private List<ADTaskProfile> adTaskProfiles;
    private String remoteAdVersion;

    public ADTaskProfileNodeResponse(DiscoveryNode node, List<ADTaskProfile> adTaskProfile, String remoteAdVersion) {
        super(node);
        this.adTaskProfiles = adTaskProfile;
        this.remoteAdVersion = remoteAdVersion;
    }

    public ADTaskProfileNodeResponse(StreamInput in, String remoteAdVersion) throws IOException {
        super(in);

        if (in.readBoolean()) {
//            byte[] buffer = new byte[Integer.BYTES];
//            in.read(buffer);
            if ("1.0.0.0".equals(remoteAdVersion)) {
                logger.info("6666666666666666666 ylwudebu11: remote node AD version is 1.0.0.0, just parse 1 profile" );
                ADTaskProfile adTaskProfile = new ADTaskProfile(in);
                logger.info("6666666666666666666 ylwudebu11: remote node AD version is 1.0.0.0, just parse 1 profile : " + adTaskProfile );
                this.adTaskProfiles = ImmutableList.of(adTaskProfile);
            } else {
                logger.info("6666666666666666666 ylwudebu11: remote node AD version is not {}, just parse 1 profile", remoteAdVersion );
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
        if ("1.0.0.0".equals(remoteAdVersion)) {
            logger.info("777777777777777 ylwudebu11: remote node AD version is 1.0.0.0, just output 1 profile" );
            ADTaskProfile detectorTaskProfile = null;
            if (adTaskProfiles != null && adTaskProfiles.size() > 0) {
                List<ADTaskProfile> profiles = adTaskProfiles.stream().filter(t -> t.getAdTask() != null && t.getAdTask().getParentTaskId() == null).collect(Collectors.toList());
                if (profiles.size() > 0) {
                    detectorTaskProfile = profiles.get(0);
                }
            }
            logger.info("777777777777777 ylwudebu11: remote node AD version is 1.0.0.0, just output 1 profile, detectorTaskProfile: " + detectorTaskProfile);
            if (detectorTaskProfile != null) {
                out.writeBoolean(true);
                detectorTaskProfile.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        } else {
            logger.info("777777777777777 ylwudebu1122222222: remote node AD version is not 1.0.0.0, will output all profiles" );
            if (adTaskProfiles != null && adTaskProfiles.size() > 0) {
                out.writeList(adTaskProfiles);
            } else {
                out.writeBoolean(false);
            }
        }

//        if (adTaskProfiles != null && adTaskProfiles.size() > 0) {
////            out.writeBoolean(true);
////            if (adTaskProfiles.size() == 1) {
////                adTaskProfiles.get(0).writeTo(out);
////            } else {
////                out.writeList(adTaskProfiles);
////            }
//            out.writeList(adTaskProfiles);
//        } else {
//            out.writeBoolean(false);
//        }
    }

    public static ADTaskProfileNodeResponse readNodeResponse(StreamInput in, String remoteAdVersion) throws IOException {
        return new ADTaskProfileNodeResponse(in, remoteAdVersion);
    }

    public static ADTaskProfileNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ADTaskProfileNodeResponse(in, null);
    }
}
