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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskAction;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.DetectionDateRange;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

public class ForwardADTaskRequest extends ActionRequest {
    private AnomalyDetector detector;
    private ADTask adTask;
    private DetectionDateRange detectionDateRange;
    private List<String> staleRunningEntities;
    private User user;
    private ADTaskAction adTaskAction;

    public ForwardADTaskRequest(
        AnomalyDetector detector,
        ADTask adTask,
        DetectionDateRange detectionDateRange,
        List<String> staleRunningEntities,
        User user,
        ADTaskAction adTaskAction
    ) {
        this.detector = detector;
        this.adTask = adTask;
        this.detectionDateRange = detectionDateRange;
        this.staleRunningEntities = staleRunningEntities;
        this.user = user;
        this.adTaskAction = adTaskAction;
    }

    public ForwardADTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.detector = new AnomalyDetector(in);
        if (in.readBoolean()) {
            this.adTask = new ADTask(in);
        }
        if (in.readBoolean()) {
            this.detectionDateRange = new DetectionDateRange(in);
        }
        this.staleRunningEntities = in.readOptionalStringList();
        if (in.readBoolean()) {
            this.user = new User(in);
        }
        this.adTaskAction = in.readEnum(ADTaskAction.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        if (adTask != null) {
            out.writeBoolean(true);
            adTask.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        if (detectionDateRange != null) {
            out.writeBoolean(true);
            detectionDateRange.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalStringCollection(staleRunningEntities);
        if (user != null) {
            out.writeBoolean(true);
            user.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeEnum(adTaskAction);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (detector == null) {
            validationException = addValidationError(CommonErrorMessages.DETECTOR_MISSING, validationException);
        } else if (detector.getDetectorId() == null) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (adTaskAction == null) {
            validationException = addValidationError(CommonErrorMessages.AD_TASK_ACTION_MISSING, validationException);
        }
        if (adTaskAction == ADTaskAction.CLEAN_RUNNING_ENTITY && (staleRunningEntities == null || staleRunningEntities.isEmpty())) {
            validationException = addValidationError(CommonErrorMessages.EMPTY_STALE_RUNNING_ENTITIES, validationException);
        }
        return validationException;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public ADTask getAdTask() {
        return adTask;
    }

    public DetectionDateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public User getUser() {
        return user;
    }

    public ADTaskAction getAdTaskAction() {
        return adTaskAction;
    }

    public List<String> getStaleRunningEntities() {
        return staleRunningEntities;
    }
}
