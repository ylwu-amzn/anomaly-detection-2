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
import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.ad.cluster.ADVersionUtil;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADTaskProfileNodeResponse extends BaseNodeResponse {
    private static final Logger logger = LogManager.getLogger(ADTaskProfileNodeResponse.class);
    private List<ADTaskProfile> adTaskProfiles;
    private Version remoteAdVersion;

    public ADTaskProfileNodeResponse(DiscoveryNode node, List<ADTaskProfile> adTaskProfile, Version remoteAdVersion) {
        super(node);
        this.adTaskProfiles = adTaskProfile;
        this.remoteAdVersion = remoteAdVersion;
    }

    //    public ADTaskProfileNodeResponse(StreamInput in, Version remoteAdVersion) throws IOException {
//        super(in);
//        DiscoveryNode node = this.getNode();
//        node.getId();
//        if (in.readBoolean()) {
//            logger.info("6666666666666666666666 remote ad version is :  " + remoteAdVersion.toString());
//            if (remoteAdVersion.onOrBefore(Version.V_1_0_0)) {
//                logger.info("6666666666666666666 ylwudebu11: remote node AD version is 1.0.0.0, just parse 1 profile" );
//                ADTaskProfile adTaskProfile = new ADTaskProfile(in);
//                logger.info("6666666666666666666 ylwudebu11: remote node AD version is 1.0.0.0, just parse 1 profile : " + adTaskProfile );
//                this.adTaskProfiles = ImmutableList.of(adTaskProfile);
//            } else {
//                logger.info("6666666666666666666 ylwudebu11: remote node AD version is {}, will read all profiles", remoteAdVersion );
//                this.adTaskProfiles = in.readList(ADTaskProfile::new);
//            }
//        } else {
//            this.adTaskProfiles = null;
//        }
//    }
    public ADTaskProfileNodeResponse(StreamInput in, HashRing hashRing) throws IOException {
        super(in);
        DiscoveryNode node = this.getNode();
        String remoteNodeId = node.getId();
        logger.info("66666666666666666666 hashRing is {}, remoteNodeId: {}", hashRing, remoteNodeId);
        Version remoteAdVersion = ADVersionUtil.fromString(hashRing.getAdVersion(remoteNodeId));
        if (in.readBoolean()) {
            logger.info("6666666666666666666666 remote ad version is :  " + remoteAdVersion.toString());
            if (remoteAdVersion.onOrBefore(Version.V_1_0_0)) {
                logger.info("6666666666666666666 ylwudebu11: remote node AD version is 1.0.0.0, just parse 1 profile");
                ADTaskProfile adTaskProfile = new ADTaskProfile(in);
                logger.info("6666666666666666666 ylwudebu11: remote node AD version is 1.0.0.0, just parse 1 profile : " + adTaskProfile);
                this.adTaskProfiles = ImmutableList.of(adTaskProfile);
            } else {
                logger.info("6666666666666666666 ylwudebu11: remote node AD version is {}, will read all profiles", remoteAdVersion);
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
            logger.info("777777777777777 ylwudebu11: remote node AD version is 1.0.0.0, just output 1 profile. Total task profile size: ", adTaskProfiles == null ? 0 : adTaskProfiles.size());

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
    }

//    public static ADTaskProfileNodeResponse readNodeResponse(StreamInput in, Version remoteAdVersion) throws IOException {
//        logger.info("0000000000000000000000000000000 ADTaskProfileNodeResponse readNodeResponse with version " + remoteAdVersion);
//        return new ADTaskProfileNodeResponse(in, remoteAdVersion);
//    }

    public static ADTaskProfileNodeResponse readNodeResponse(StreamInput in, HashRing hashRing) throws IOException {
        logger.info("0000000000000000000000000000000 ADTaskProfileNodeResponse readNodeResponse with current version");
        return new ADTaskProfileNodeResponse(in, hashRing);
    }
}
