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

package org.opensearch.ad.ratelimit;

import org.opensearch.ad.model.AnomalyResult;

public class ResultWriteRequest extends QueuedRequest {
    private final AnomalyResult result;
    private final String resultIndex;

    public ResultWriteRequest(long expirationEpochMs, String detectorId, RequestPriority priority, AnomalyResult result, String resultIndex) {
        super(expirationEpochMs, detectorId, priority);
        this.result = result;
        this.resultIndex = resultIndex;
    }

    public AnomalyResult getResult() {
        return result;
    }

    public String getResultIndex() {
        return resultIndex;
    }
}
