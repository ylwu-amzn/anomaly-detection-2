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

package org.opensearch.ad.common.exception;

/**
 * AD version incompatible exception.
 */
public class ADVersionConflictException extends AnomalyDetectionException {

    public ADVersionConflictException(String message) {
        super(message);
    }

    public ADVersionConflictException(String anomalyDetectorId, String message) {
        super(anomalyDetectorId, message);
    }

    public ADVersionConflictException(String anomalyDetectorId, String message, Throwable throwable) {
        super(anomalyDetectorId, message, throwable);
    }

    public ADVersionConflictException(String anomalyDetectorId, Throwable cause) {
        super(anomalyDetectorId, cause);
    }
}
