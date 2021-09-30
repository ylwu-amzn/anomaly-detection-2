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

package org.opensearch.ad.cluster;

import org.opensearch.Version;

public class ADNodeInfo {
    private Version version;
    private boolean isEligibleDataNode;

    public ADNodeInfo(Version version, boolean isEligibleDataNode) {
        this.version = version;
        this.isEligibleDataNode = isEligibleDataNode;
    }

    public Version getVersion() {
        return version;
    }

    public boolean isEligibleDataNode() {
        return isEligibleDataNode;
    }

    @Override
    public String toString() {
        return "ADNodeInfo{" +
                "version=" + version +
                ", isEligibleDataNode=" + isEligibleDataNode +
                '}';
    }
}
