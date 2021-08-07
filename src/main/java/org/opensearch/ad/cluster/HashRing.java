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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.ad.common.exception.ADVersionConflictException;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.plugins.PluginInfo;

public class HashRing {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private static final Logger LOG = LogManager.getLogger(HashRing.class);
    static final String REBUILD_MSG = "Rebuild hash ring";
    // In case of frequent node join/leave, hash ring has a cooldown period say 5 minute.
    // Hash ring doesn't respond to more than 1 cluster membership changes within the
    // cool-down period.
    static final String COOLDOWN_MSG = "Hash ring doesn't respond to cluster state change within the cooldown period.";

    private final int VIRTUAL_NODE_COUNT = 100;
    private final DiscoveryNodeFilterer nodeFilter;
    private TreeMap<Integer, DiscoveryNode> circle;
    private Semaphore inProgress;
    // the UTC epoch milliseconds of the most recent successful update
    private long lastUpdate;
    private final TimeValue coolDownPeriod;
    private final Clock clock;
    private AtomicBoolean membershipChangeRequied;
    private final Client client;
    private Map<String, String> nodeAdVersions;
    private TreeMap<String, TreeMap<Integer, DiscoveryNode>> adVersionCircles;
    private ClusterService clusterService;
    private ADDataMigrator dataMigrator;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices detectionIndices;

    public HashRing(DiscoveryNodeFilterer nodeFilter, Clock clock, Settings settings, Client client, ClusterService clusterService, NamedXContentRegistry xContentRegistry, AnomalyDetectionIndices detectionIndices, ADDataMigrator dataMigrator) {
        this.circle = new TreeMap<>();
        this.nodeFilter = nodeFilter;
        this.inProgress = new Semaphore(1);
        this.clock = clock;
        this.coolDownPeriod = COOLDOWN_MINUTES.get(settings);
        this.lastUpdate = 0;
        this.membershipChangeRequied = new AtomicBoolean(false);
        this.client = client;
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.dataMigrator = dataMigrator;
        this.nodeAdVersions = new ConcurrentHashMap<>();
        this.adVersionCircles = new TreeMap<>();
    }

    /**
     * Rebuilds the hash ring when cluster membership change is required.
     * The build method skips a rebuilding if it has already rebuilt the hash ring within the
     *  cooldown period or the rebuilding is already in progress.
     * @return whether hash ring is rebuilt or not.
     */
    public boolean build() {
        // Check this conjunct first since most of time this conjunct evaluates
        // to false and we can skip of the following checks.
        // Hash ring can be empty because we cannot build the ring in constructor. The constructor
        // is called when the plugin is being loaded. At that time, cluster state is empty.
        if (!membershipChangeRequied.get() && !circle.isEmpty()) {
            return false;
        }

        // Check cooldown period
        if (clock.millis() - lastUpdate <= coolDownPeriod.getMillis()) {
            LOG.debug(COOLDOWN_MSG);
            return false;
        }

        // When the condition check passes, we start hash ring rebuilding.
        if (!inProgress.tryAcquire()) {
            LOG.info("Hash ring change in progress, return.");
            return false;
        }

        LOG.info(REBUILD_MSG);
        TreeMap<Integer, DiscoveryNode> newCircle = new TreeMap<>();

        try {
            for (DiscoveryNode curNode : nodeFilter.getEligibleDataNodes()) {
                for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                    newCircle.put(Murmur3HashFunction.hash(curNode.getId() + i), curNode);
                }
            }
            circle = newCircle;
            buildCirclesOnAdVersions();
            lastUpdate = clock.millis();
            membershipChangeRequied.set(false);
        } catch (Exception ex) {
            LOG.error("Hash ring cannot be rebuilt", ex);
            return false;
        } finally {
            inProgress.release();
        }
        return true;
    }

    private void buildCirclesOnAdVersions() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear().addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        client.admin().cluster().nodesInfo(nodesInfoRequest, ActionListener.wrap(r -> {
            logger.error("99999999998888888888 ylwudebug1: start to get node info to build hash ring");
            Map<String, NodeInfo> nodesMap = r.getNodesMap();
            if (nodesMap != null && nodesMap.size() > 0) {
                for (Map.Entry<String, NodeInfo> entry : nodesMap.entrySet()) {
                    NodeInfo nodeInfo = entry.getValue();
                    PluginsAndModules plugins = nodeInfo.getInfo(PluginsAndModules.class);
                    if (plugins == null) {
                        continue;
                    }
                    DiscoveryNode curNode = nodeInfo.getNode();
                    TreeMap<Integer, DiscoveryNode> circle = null;
                    for (PluginInfo pluginInfo : plugins.getPluginInfos()) {
                        if ("opensearch-anomaly-detection".equals(pluginInfo.getName())) {
                            circle = adVersionCircles.computeIfAbsent(pluginInfo.getVersion(), key -> new TreeMap<>());
                            circle.clear();
                            nodeAdVersions.computeIfAbsent(curNode.getId(), key -> pluginInfo.getVersion());
                        }
                    }
                    if (circle != null) {
                        for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                            circle.put(Murmur3HashFunction.hash(curNode.getId() + i), curNode);
                        }
                    }
                }
            }
                    if (adVersionCircles.containsKey("1.0.0.0")) {
                        dataMigrator.migrateData();
                    }
//            if (!dataMigrated.get() && adVersionCircles.containsKey("1.0.0.0")) {
//                logger.info("000000000000000000000000000000000000000000000000000000000000 start to backfilltask");
//                if (detectionIndices.doesDetectorStateIndexExist()) {
//                    migrateData();
//                } else {
//                    // If detection index doesn't exist, create index and execute historical detector.
//                    detectionIndices.initDetectionStateIndex(ActionListener.wrap(res -> {
//                        if (res.isAcknowledged()) {
//                            logger.info("Created {} with mappings.", CommonName.DETECTION_STATE_INDEX);
//                            migrateData();
//                        } else {
//                            String error = "Create index " + CommonName.DETECTION_STATE_INDEX + " with mappings not acknowledged";
//                            logger.warn(error);
//                        }
//                    }, e -> {
//                        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
//                            migrateData();
//                        } else {
//                            logger.error("Failed to init anomaly detection state index", e);
//                        }
//                    }));
//                }
//
//            }
            logger.info("111111111111111111111111111111111111111111111111111111111111");
            logger.info(Arrays.toString(adVersionCircles.keySet().toArray(new String[0])));
        }, e -> {
            logger.error("99999999998888888888 ylwudebug1: failed to get node info to build hash ring", e);
        }));
    }

//    private void migrateData() {
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(MAX_DETECTOR_UPPER_LIMIT);
//        SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTOR_JOB_INDEX).source(searchSourceBuilder);
//        //TODO: check if realtime task exists in AD job runner.
//        client.search(searchRequest, ActionListener.wrap(r -> {
//            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
//                logger.info("000000000000000000000000000000000000000000000000000000000000 No anomaly detector job found, will skip migrating data");
//                return;
//            }
//            logger.info("000000000000000000000000000000000000000000000000000000000000 job count : {}", r.getHits().getTotalHits());
//            ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs = new ConcurrentLinkedQueue<>();
//            Iterator<SearchHit> iterator = r.getHits().iterator();
//            while (iterator.hasNext()) {
//                SearchHit searchHit = iterator.next();
//                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
//                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
//                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
//                    detectorJobs.add(job);
//                } catch (IOException e) {
//                    logger.error("Fail to parse AD job " + searchHit.getId(), e);
//                }
//            }
//            logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask total jobs : {}", detectorJobs.size());
//            backfillRealtimeTask(detectorJobs);
//        }, e->{
//            if (!(e instanceof IndexNotFoundException)) {
//                logger.error("Failed to migrate AD data", e);
//            }
//        }));
//    }

//    public void backfillRealtimeTask(ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs) {
//        AnomalyDetectorJob job = detectorJobs.poll();
//        if (job == null) {
//            return;
//        }
//        String jobId = job.getName();
//        GetRequest getRequest = new GetRequest(DETECTION_STATE_INDEX, jobId);
//        client.get(getRequest, ActionListener.wrap(r -> {
//            if (r != null && r.isExists()) {
//                logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask state exists for "+jobId);
//                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
//                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
//                    DetectorInternalState detectorState = DetectorInternalState.parse(parser);
//                    createRealtimeADTask(job, detectorState.getError(), detectorJobs);
//                } catch (IOException e) {
//                    logger.error("1111111111111backfilltask " + jobId, e);
//                    createRealtimeADTask(job, null, detectorJobs);
//                }
//            } else {
//                logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask state exists not for "+jobId);
//                createRealtimeADTask(job, null, detectorJobs);
//            }
//        }, e -> {
//            logger.error("1111111111111backfilltask22 " + jobId, e);
//            createRealtimeADTask(job, null, detectorJobs);
//        }));
//    }

//    private void createRealtimeADTask(AnomalyDetectorJob job, String error, ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs) {
//        logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask start to create task for "+job.getName());
//        client.get(new GetRequest(ANOMALY_DETECTORS_INDEX, job.getName()), ActionListener.wrap(r -> {
//            if (r != null && r.isExists()) {
//                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
//                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
//                    AnomalyDetector detector = AnomalyDetector.parse(parser, r.getId());
//                    ADTaskType taskType = detector.isMultientityDetector() ? ADTaskType.REALTIME_HC_DETECTOR : ADTaskType.REALTIME_SINGLE_ENTITY;
//                    Instant now = Instant.now();
//                    String userName = job.getUser() != null ? job.getUser().getName() : null;
//                    ADTask adTask = new ADTask.Builder()
//                            .detectorId(detector.getDetectorId())
//                            .detector(detector)
//                            .isLatest(true)
//                            .taskType(taskType.name())
//                            .executionStartTime(now)
//                            .taskProgress(0.0f)
//                            .initProgress(0.0f)
//                            .state(ADTaskState.CREATED.name())
//                            .lastUpdateTime(now)
//                            .startedBy(userName)
//                            .coordinatingNode(clusterService.localNode().getId())
//                            .detectionDateRange(null)
//                            .user(job.getUser())
//                            .build();
//                    IndexRequest indexRequest = new IndexRequest(DETECTION_STATE_INDEX)
//                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
//                            .source(adTask.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE));
//                    logger.info("000000000000000000000000000000000000000000000000000000000000backfilltask start to create task22 for "+job.getName());
//                    client.index(indexRequest, ActionListener.wrap(indexResponse -> {
//                        logger.info("1111111111111backfilltask Backfilled realtime task successfully for detector {}", job.getName());
//                        backfillRealtimeTask(detectorJobs);
//                    }, ex -> {
//                        logger.error("1111111111111backfilltask Failed to backfill realtime task for detector "+job.getName(), ex);
//                        backfillRealtimeTask(detectorJobs);
//                    }));
//                } catch (IOException e) {
//                    logger.error("1111111111111backfilltask " + job.getName(), e);
//                    backfillRealtimeTask(detectorJobs);
//                }
//            } else {
//                logger.error("1111111111111backfilltask job doesn't exist" + job.getName());
//                backfillRealtimeTask(detectorJobs);
//            }
//        }, e -> {
//            logger.error("1111111111111backfilltask " + job.getName(), e);
//            backfillRealtimeTask(detectorJobs);
//        }));
//
//    }

    /**
     * Compute the owning node of modelID using consistent hashing
     * @param modelId example: http-latency-rcf-1
     * @return the owning node of the modeID
     */
    public Optional<DiscoveryNode> getOwningNode(String modelId) {
        build();

        int modelHash = Murmur3HashFunction.hash(modelId);
        Map.Entry<Integer, DiscoveryNode> entry = circle.higherEntry(modelHash);

        // The method can return an empty Optional. Say two concurrent getOwningNode requests to
        // the hash ring before it's been built. The first one starts building it,
        // turning on inProgress. The second one returns from build and continues on to
        // the rest of hashing and look up while the ring is still being built and thus empty.
        // The second getOwningNode request returns an empty Optional in this case.
        return Optional.ofNullable(Optional.ofNullable(entry).orElse(circle.firstEntry())).map(x -> x.getValue());
    }

    public Optional<DiscoveryNode> getOwningNodeWithSameAdVersion(String modelId, String nodeId) {
        build();

        int modelHash = Murmur3HashFunction.hash(modelId);
        String adVersion = nodeAdVersions.get(nodeId);
        Map.Entry<Integer, DiscoveryNode> entry = adVersionCircles.get(adVersion).higherEntry(modelHash);

        // The method can return an empty Optional. Say two concurrent getOwningNode requests to
        // the hash ring before it's been built. The first one starts building it,
        // turning on inProgress. The second one returns from build and continues on to
        // the rest of hashing and look up while the ring is still being built and thus empty.
        // The second getOwningNode request returns an empty Optional in this case.
        return Optional.ofNullable(Optional.ofNullable(entry).orElse(circle.firstEntry())).map(x -> x.getValue());
    }

    public Set<DiscoveryNode> getNodesWithSameAdVersion(DiscoveryNode node) {
        String adVersion = nodeAdVersions.get(node.getId());
        TreeMap<Integer, DiscoveryNode> circle = adVersionCircles.get(adVersion);
        Set<String> nodeIds = new HashSet<>();
        Set<DiscoveryNode> nodes = new HashSet<>();
        nodeIds.add(node.getId());
        nodes.add(node);
        if (circle == null) {
            return nodes;
        }
        circle.entrySet().stream().forEach(e -> {
            DiscoveryNode discoveryNode = e.getValue();
            if (!nodeIds.contains(discoveryNode.getId())) {
                nodeIds.add(discoveryNode.getId());
                nodes.add(discoveryNode);
            }
        });
        return nodes;
    }

    public DiscoveryNode[] getNodesWithSameLocalAdVersion() {
        DiscoveryNode localNode = clusterService.localNode();
        return getNodesWithSameAdVersion(localNode).toArray(new DiscoveryNode[0]);
    }

    public void recordMembershipChange() {
        membershipChangeRequied.set(true);
    }

    public String getAdVersion(String nodeId) {
        return nodeAdVersions.get(nodeId);
    }

    public boolean hasSameAdVersion(String nodeId, String otherNodeId) {
        return Objects.equals(nodeAdVersions.get(nodeId), nodeAdVersions.get(otherNodeId));
    }

    public void validateAdVersion(String nodeId, String remoteNodeId) {
        if (!hasSameAdVersion(nodeId, remoteNodeId)) {
            throw new ADVersionConflictException("Different AD version on remote node " + remoteNodeId + ". Local node AD version: " + getAdVersion(nodeId) + ", remote node AD version: " + getAdVersion(remoteNodeId));
        }
    }

}
