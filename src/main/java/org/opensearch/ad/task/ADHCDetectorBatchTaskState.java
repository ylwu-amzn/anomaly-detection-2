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

package org.opensearch.ad.task;

import org.apache.lucene.util.SetOnce;
import org.opensearch.ad.model.ADTaskState;

public class ADHCDetectorBatchTaskState {

    private String detectorTaskState;
//    private SetOnce<Boolean> isCoordinatingNode;
    private String cancelReason;
    private String cancelledBy;

    public ADHCDetectorBatchTaskState() {
        this.detectorTaskState = ADTaskState.INIT.name();
//        this.isCoordinatingNode = new SetOnce<>();
    }

    public String getDetectorTaskState() {
        return detectorTaskState;
    }

    public void setDetectorTaskState(String detectorTaskState) {
        this.detectorTaskState = detectorTaskState;
    }

//    public void setIsCoordinatingNode(boolean isCoordinatingNode) {
//        this.isCoordinatingNode.set(isCoordinatingNode);
//    }
//
//    public boolean isCoordinatingNode() {
//        Boolean isCoordinating = this.isCoordinatingNode.get();
//        return isCoordinating != null && isCoordinating.booleanValue();
//    }


    public String getCancelReason() {
        return cancelReason;
    }

    public void setCancelReason(String cancelReason) {
        this.cancelReason = cancelReason;
    }

    public String getCancelledBy() {
        return cancelledBy;
    }

    public void setCancelledBy(String cancelledBy) {
        this.cancelledBy = cancelledBy;
    }
}
