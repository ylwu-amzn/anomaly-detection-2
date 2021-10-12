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

package org.opensearch.ad.transport;

import static org.opensearch.ad.constant.CommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.model.ADTask.PARENT_TASK_ID_FIELD;
import static org.opensearch.ad.model.ADTask.STATE_FIELD;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INDEX_PRESSURE_HARD_LIMIT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT;
import static org.opensearch.ad.util.ParseUtils.isNullOrEmpty;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.IndexingPressure.MAX_INDEXING_BYTES;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.BulkUtil;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.IndexingPressure;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class ADResultBulkTransportAction extends HandledTransportAction<ADResultBulkRequest, ADResultBulkResponse> {

    private static final Logger LOG = LogManager.getLogger(ADResultBulkTransportAction.class);
    private IndexingPressure indexingPressure;
    private final long primaryAndCoordinatingLimits;
    private float softLimit;
    private float hardLimit;
    private String indexName;
    private Client client;
    private Random random;
    private final AnomalyDetectionIndices detectionIndices;
    private TransportService transportService;
    private ADTaskManager adTaskManager;

    @Inject
    public ADResultBulkTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        Settings settings,
        ClusterService clusterService,
        Client client,
        AnomalyDetectionIndices detectionIndices,
        ADTaskManager adTaskManager
    ) {
        super(ADResultBulkAction.NAME, transportService, actionFilters, ADResultBulkRequest::new, ThreadPool.Names.SAME);
        this.indexingPressure = indexingPressure;
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.softLimit = INDEX_PRESSURE_SOFT_LIMIT.get(settings);
        this.hardLimit = INDEX_PRESSURE_HARD_LIMIT.get(settings);
        //TODO: support custom index
        this.indexName = CommonName.ANOMALY_RESULT_INDEX_ALIAS;
        this.client = client;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDEX_PRESSURE_SOFT_LIMIT, it -> softLimit = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INDEX_PRESSURE_HARD_LIMIT, it -> hardLimit = it);
        // random seed is 42. Can be any number
        this.random = new Random(42);
        this.detectionIndices = detectionIndices;
        this.adTaskManager = adTaskManager;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, ADResultBulkRequest request, ActionListener<ADResultBulkResponse> listener) {
        // Concurrent indexing memory limit = 10% of heap
        // indexing pressure = indexing bytes / indexing limit
        // Write all until index pressure (global indexing memory pressure) is less than 80% of 10% of heap. Otherwise, index
        // all non-zero anomaly grade index requests and index zero anomaly grade index requests with probability (1 - index pressure).
        long totalBytes = indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes() + indexingPressure.getCurrentReplicaBytes();
        float indexingPressurePercent = (float) totalBytes / primaryAndCoordinatingLimits;
        List<AnomalyResult> results = request.getAnomalyResults();
        String resultIndex = request.getResultIndex();
        resultIndex = Strings.isNotBlank(resultIndex) ? resultIndex : indexName;

        if (results == null || results.size() < 1) {
            listener.onResponse(new ADResultBulkResponse());
        }

        String detectorId = results.get(0).getDetectorId();

        BulkRequest bulkRequest = new BulkRequest();

        if (indexingPressurePercent <= softLimit) {
            for (AnomalyResult result : results) {
                addResult(bulkRequest, result, resultIndex);
            }
        } else if (indexingPressurePercent <= hardLimit) {
            // exceed soft limit (60%) but smaller than hard limit (90%)
            float acceptProbability = 1 - indexingPressurePercent;
            for (AnomalyResult result : results) {
                if (result.isHighPriority() || random.nextFloat() < acceptProbability) {
                    addResult(bulkRequest, result, resultIndex);
                }
            }
        } else {
            // if exceeding hard limit, only index non-zero grade or error result
            for (AnomalyResult result : results) {
                if (result.isHighPriority()) {
                    addResult(bulkRequest, result, resultIndex);
                }
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
            if (!detectionIndices.doesIndexExist(resultIndex)) {
                if (!adTaskManager.hasRealtimeTaskCache(detectorId)) {
                    LOG.warn("-------------------00000000001111111115555555555 realtime task cache doesn't exists: {}", resultIndex);
                    listener.onResponse(new ADResultBulkResponse());
                    return;
                }
                LOG.warn("-------------------0000000000111111111 result index doesn't exist: {}", resultIndex);
                UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
                updateByQueryRequest.indices(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX);
                BoolQueryBuilder query = new BoolQueryBuilder();
                query.filter(new TermQueryBuilder(AnomalyDetectorJob.NAME_FIELD, detectorId));
                updateByQueryRequest.setQuery(query);
                updateByQueryRequest.setRefresh(true);
                String script = String.format(Locale.ROOT, "ctx._source.%s=%s;", AnomalyDetectorJob.IS_ENABLED_FIELD, false);
                updateByQueryRequest.setScript(new Script(script));

                String finalResultIndex = resultIndex;
                adTaskManager.stopLatestRealtimeTask(detectorId, ADTaskState.STOPPED, null, transportService, ActionListener.wrap(response -> {
                    client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
                        // TODO: remove this, recreate result index if missing,
                        LOG.warn("-------------------000000000011111111122222222222 disabled realtime job successfully for detector " + detectorId);
                        listener.onFailure(new EndRunException(detectorId, "can't find result index " + finalResultIndex, false));
                        adTaskManager.removeRealtimeTaskCache(detectorId);
                    }, e -> {
                        LOG.error("Failed to disable realtime job for " + detectorId, e);
                        listener.onFailure(new EndRunException(detectorId, "can't find result index " + finalResultIndex, true));
                    }));
                    LOG.warn("-------------------00000000001111111115555555555777777 realtime task stopped: {}", detectorId);
                }, ex-> {
                    LOG.warn("-------------------00000000001111111115555555555777777 realtime task failed to stopped: " + detectorId, ex);
                }));
            } else {
                LOG.info("-------------------0000000000 result index exists: {}", resultIndex);
                client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.<BulkResponse>wrap(bulkResponse -> {
                    List<IndexRequest> failedRequests = BulkUtil.getFailedIndexRequest(bulkRequest, bulkResponse);
                    listener.onResponse(new ADResultBulkResponse(failedRequests));
                }, e -> {
                    LOG.error("Failed to bulk index AD result", e);
                    listener.onFailure(e);
                }));
            }
        } else {
            listener.onResponse(new ADResultBulkResponse());
        }
    }

    private void addResult(BulkRequest bulkRequest, AnomalyResult result, String resultIndex) {
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(resultIndex).source(result.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            bulkRequest.add(indexRequest);
        } catch (IOException e) {
            LOG.error(String.format(Locale.ROOT, "Failed to prepare bulk %s", resultIndex), e);
        }
    }
}
