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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanQuery;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.ad.constant.CommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static org.opensearch.ad.model.ADTask.TASK_TYPE_FIELD;
import static org.opensearch.ad.model.ADTaskType.taskTypeToString;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_DETECTOR_UPPER_LIMIT;
import static org.opensearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.ad.util.RestHandlerUtils.createXContentParserFromRegistry;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ADDataMigrator {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices detectionIndices;
    private final AtomicBoolean dataMigrated;

    public ADDataMigrator(Client client, ClusterService clusterService, NamedXContentRegistry xContentRegistry, AnomalyDetectionIndices detectionIndices) {
        this.client = client;
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.dataMigrated = new AtomicBoolean(false);
    }

    public void migrateData() {
        if (!dataMigrated.get()) {
            logger.info("000000000000000000000000000000000000000000000000000000000000 start to backfilltask");

            if (detectionIndices.doesDetectorStateIndexExist()) {
                migrateDetectionStateToRealtimeTask();
            } else {
                // If detection index doesn't exist, create index and execute historical detector.
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", CommonName.DETECTION_STATE_INDEX);
                        migrateDetectionStateToRealtimeTask();
                    } else {
                        String error = "Create index " + CommonName.DETECTION_STATE_INDEX + " with mappings not acknowledged";
                        logger.warn(error);
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        migrateDetectionStateToRealtimeTask();
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                    }
                }));
            }
        }
    }

    public void migrateDetectionStateToRealtimeTask() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(MAX_DETECTOR_UPPER_LIMIT);
        SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTOR_JOB_INDEX).source(searchSourceBuilder);
        //TODO: check if realtime task exists in AD job runner.
        client.search(searchRequest, ActionListener.wrap(r -> {
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                logger.info("000000000000000000000000000000000000000000000000000000000000 No anomaly detector job found, will skip migrating data");
                return;
            }
            logger.info("000000000000000000000000000000000000000000000000000000000000 job count : {}", r.getHits().getTotalHits());
            ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs = new ConcurrentLinkedQueue<>();
            Iterator<SearchHit> iterator = r.getHits().iterator();
            while (iterator.hasNext()) {
                SearchHit searchHit = iterator.next();
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    detectorJobs.add(job);
                } catch (IOException e) {
                    logger.error("Fail to parse AD job " + searchHit.getId(), e);
                }
            }
            logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask total jobs : {}", detectorJobs.size());
            backfillRealtimeTask(detectorJobs);
        }, e->{
            if (!(e instanceof IndexNotFoundException)) {
                logger.error("Failed to migrate AD data", e);
            }
        }));
    }

    public void backfillRealtimeTask(ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs) {
        AnomalyDetectorJob job = detectorJobs.poll();
        if (job == null) {
            return;
        }
        String jobId = job.getName();

        AnomalyDetectorFunction function = () -> {
            GetRequest getRequest = new GetRequest(DETECTION_STATE_INDEX, jobId);
            client.get(getRequest, ActionListener.wrap(r -> {
                if (r != null && r.isExists()) {
                    logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask state exists for "+jobId);
                    try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        DetectorInternalState detectorState = DetectorInternalState.parse(parser);
                        createRealtimeADTask(job, detectorState.getError(), detectorJobs);
                    } catch (IOException e) {
                        logger.error("1111111111111backfilltask " + jobId, e);
                        createRealtimeADTask(job, null, detectorJobs);
                    }
                } else {
                    logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask state exists not for "+jobId);
                    createRealtimeADTask(job, null, detectorJobs);
                }
            }, e -> {
                logger.error("1111111111111backfilltask22 " + jobId, e);
                createRealtimeADTask(job, null, detectorJobs);
            }));
        };
        checkIfRealtimeTaskExistsAndBackfill(jobId, function);
    }

    private void checkIfRealtimeTaskExistsAndBackfill(String jobId, AnomalyDetectorFunction function) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, jobId));
        query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(ADTaskType.REALTIME_TASK_TYPES)));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(new TermQueryBuilder(DETECTOR_ID_FIELD, jobId))
                .size(1);
        SearchRequest searchRequest = new SearchRequest(DETECTION_STATE_INDEX).source(searchSourceBuilder);
        client.search(searchRequest, ActionListener.wrap(r -> {
            if (r != null && r.getHits().getTotalHits().value > 0) {
                logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask task exists, no need to backfill "+jobId);
                return;
            }
            function.execute();
        }, e -> {
            logger.error("Failed to search tasks of detector " + jobId);
        }));
    }

    private void createRealtimeADTask(AnomalyDetectorJob job, String error, ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs) {
        logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask start to create task for "+job.getName());
        client.get(new GetRequest(ANOMALY_DETECTORS_INDEX, job.getName()), ActionListener.wrap(r -> {
            if (r != null && r.isExists()) {
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetector detector = AnomalyDetector.parse(parser, r.getId());
                    ADTaskType taskType = detector.isMultientityDetector() ? ADTaskType.REALTIME_HC_DETECTOR : ADTaskType.REALTIME_SINGLE_ENTITY;
                    Instant now = Instant.now();
                    String userName = job.getUser() != null ? job.getUser().getName() : null;
                    ADTask adTask = new ADTask.Builder()
                            .detectorId(detector.getDetectorId())
                            .detector(detector)
                            .isLatest(true)
                            .taskType(taskType.name())
                            .executionStartTime(now)
                            .taskProgress(0.0f)
                            .initProgress(0.0f)
                            .state(ADTaskState.CREATED.name())
                            .lastUpdateTime(now)
                            .startedBy(userName)
                            .coordinatingNode(clusterService.localNode().getId())
                            .detectionDateRange(null)
                            .user(job.getUser())
                            .build();
                    IndexRequest indexRequest = new IndexRequest(DETECTION_STATE_INDEX)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                            .source(adTask.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE));
                    logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask start to create task22 for "+job.getName());
                    client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                        logger.info("1111111111111backfilltask Backfilled realtime task successfully for detector {}", job.getName());
                        backfillRealtimeTask(detectorJobs);
                    }, ex -> {
                        logger.error("1111111111111backfilltask Failed to backfill realtime task for detector "+job.getName(), ex);
                        backfillRealtimeTask(detectorJobs);
                    }));
                } catch (IOException e) {
                    logger.error("1111111111111backfilltask " + job.getName(), e);
                    backfillRealtimeTask(detectorJobs);
                }
            } else {
                logger.error("1111111111111backfilltask job doesn't exist" + job.getName());
                backfillRealtimeTask(detectorJobs);
            }
        }, e -> {
            logger.error("1111111111111backfilltask " + job.getName(), e);
            backfillRealtimeTask(detectorJobs);
        }));
    }
}
