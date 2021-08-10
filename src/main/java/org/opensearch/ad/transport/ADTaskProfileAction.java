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

import static org.opensearch.ad.constant.CommonName.AD_TASK;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionType;
import org.opensearch.ad.AnomalyDetectorJobRunner;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.constant.CommonValue;

public class ADTaskProfileAction extends ActionType<ADTaskProfileResponse> {
    private static final Logger logger = LogManager.getLogger(AnomalyDetectorJobRunner.class);
    public static final String NAME = CommonValue.INTERNAL_ACTION_PREFIX + "detectors/profile/" + AD_TASK;
    public static ADTaskProfileAction INSTANCE;

    public static ADTaskProfileAction getADTaskProfileActionInstance(HashRing hashRing) {
        logger.info("yyyyyyyyyyyyyyyyywwwwwwwwwwwwwwwwwwwwwww getADTaskProfileActionInstance");
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (ADTaskProfileAction.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new ADTaskProfileAction(hashRing);
            return INSTANCE;
        }
    }

    private ADTaskProfileAction(HashRing hashRing) {
        super(NAME, input -> new ADTaskProfileResponse(input, hashRing));
    }

}
