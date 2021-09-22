/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.ad.bwc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.ADRestTestUtils;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.google.common.collect.ImmutableMap;

import static org.opensearch.ad.TestHelpers.parser;
import static org.opensearch.ad.rest.ADRestTestUtils.DetectorType.MULTI_CATEGORY_HC_DETECTOR;
import static org.opensearch.ad.rest.ADRestTestUtils.DetectorType.SINGLE_CATEGORY_HC_DETECTOR;
import static org.opensearch.ad.rest.ADRestTestUtils.DetectorType.SINGLE_ENTITY_DETECTOR;
import static org.opensearch.ad.rest.ADRestTestUtils.countADResultOfDetector;
import static org.opensearch.ad.rest.ADRestTestUtils.countDetectors;
import static org.opensearch.ad.rest.ADRestTestUtils.createAnomalyDetector;
import static org.opensearch.ad.rest.ADRestTestUtils.ingestTestDataForHistoricalAnalysis;
import static org.opensearch.ad.rest.ADRestTestUtils.searchLatestAdTaskOfDetector;

public class ADBackwardsCompatibilityIT extends OpenSearchRestTestCase {

    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");
    private String dataIndexName = "test_data_for_ad_plugin";
    private int detectionIntervalInMinutes = 1;
    private int windowDelayIntervalInMinutes = 1;
    private String aggregationMethod = "sum";
    private int totalDocs = 10_000;
    private int categoryFieldSize = 2;

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings
            .builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .put("logger.org.opensearch.ad.task.ADBatchTaskRunner", "DEBUG")
            .put("logger.org.opensearch.ad.task.ADTaskCacheManager", "DEBUG")
            .put("logger.org.opensearch.ad.task.ADTaskManager", "DEBUG")
            .put("logger.org.opensearch.ad.transport.ForwardADTaskTransportAction", "DEBUG")
            .put("logger.com.amazon.opendistroforelasticsearch.ad.task.ADBatchTaskRunner", "DEBUG")
            .put("logger.com.amazon.opendistroforelasticsearch.ad.task.ADTaskCacheManager", "DEBUG")
            .put("logger.com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager", "DEBUG")
            .put("logger.com.amazon.opendistroforelasticsearch.ad.transport.ForwardADTaskTransportAction", "DEBUG")
            .build();
    }

    private enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

//    private Response ingestSimpleMockLog(
//            String indexName,
//            int startDays,
//            int totalDoc,
//            long intervalInMinutes,
//            ToDoubleFunction<Integer> valueFunc,
//            int ipSize,
//            int categorySize
//    ) throws IOException {
//        TestHelpers
//                .makeRequest(
//                        client(),
//                        "PUT",
//                        indexName,
//                        null,
//                        TestHelpers.toHttpEntity(MockSimpleLog.INDEX_MAPPING),
//                        ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
//                );
//
//        Response statsResponse = TestHelpers.makeRequest(client(), "GET", indexName, ImmutableMap.of(), "", null);
//        assertEquals(RestStatus.OK, TestHelpers.restStatus(statsResponse));
//        String result = EntityUtils.toString(statsResponse.getEntity());
//        assertTrue(result.contains(indexName));
//
//        StringBuilder bulkRequestBuilder = new StringBuilder();
//        Instant startTime = Instant.now().minus(startDays, ChronoUnit.DAYS);
//        for (int i = 0; i < totalDoc; i++) {
//            for (int m = 0; m < ipSize; m++) {
//                String ip = "192.168.1." + m;
//                for (int n = 0; n < categorySize; n++) {
//                    String category = "category" + n;
//                    String docId = randomAlphaOfLength(10);
//                    bulkRequestBuilder.append("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"" + docId + "\" } }\n");
//                    MockSimpleLog simpleLog1 = new MockSimpleLog(
//                            startTime,
//                            valueFunc.applyAsDouble(i),
//                            ip,
//                            category,
//                            randomBoolean(),
//                            randomAlphaOfLength(5)
//                    );
//                    bulkRequestBuilder.append(TestHelpers.toJsonString(simpleLog1));
//                    bulkRequestBuilder.append("\n");
//                }
//            }
//            startTime = startTime.plus(intervalInMinutes, ChronoUnit.MINUTES);
//        }
//        Response bulkResponse = TestHelpers
//                .makeRequest(
//                        client(),
//                        "POST",
//                        "_bulk?refresh=true",
//                        null,
//                        TestHelpers.toHttpEntity(bulkRequestBuilder.toString()),
//                        ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
//                );
//        return bulkResponse;
//    }
//
//    private void ingestTestDataForHistoricalAnalysis(String indexName, int detectionIntervalInMinutes) throws IOException {
//        ingestSimpleMockLog(indexName, 5, 6000, detectionIntervalInMinutes, (i) -> {
//            if (i % 500 == 0) {
//                return randomDoubleBetween(100, 1000, true);
//            } else {
//                return randomDoubleBetween(1, 10, true);
//            }
//        }, 2, 2);
//    }

    @SuppressWarnings("unchecked")
    public void testBackwardsCompatibility() throws Exception {
        // ingest test data
        // ingestTestDataForHistoricalAnalysis("aaaaaaaaaa", 1);
        String uri = getUri();
        Map<String, Map<String, Object>> responseMap = (Map<String, Map<String, Object>>) getAsMap(uri).get("nodes");
        for (Map<String, Object> response : responseMap.values()) {
            List<Map<String, Object>> plugins = (List<Map<String, Object>>) response.get("plugins");
            Set<Object> pluginNames = plugins.stream().map(map -> map.get("name")).collect(Collectors.toSet());
            switch (CLUSTER_TYPE) {
                case OLD:
                    // Ingest test data
                    ingestTestDataForHistoricalAnalysis(client(), dataIndexName, detectionIntervalInMinutes,
                            true, 10, totalDocs, categoryFieldSize);
                    getTestData();
                    Assert.assertTrue(pluginNames.contains("opendistro-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opendistro-job-scheduler"));
                    createRealtimeAnomalyDetectorsAndStart(SINGLE_ENTITY_DETECTOR, false);
                    createRealtimeAnomalyDetectorsAndStart(SINGLE_CATEGORY_HC_DETECTOR, false);
                    createHistoricalAnomalyDetectorsAndStart();
                    verifyAnomalyDetector(TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI);
                    assertEquals(3, countDetectors(client(), null));
                    break;
                case MIXED:
                    Assert.assertTrue(pluginNames.contains("opensearch-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    verifyAnomalyDetector(TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI);
                    createRealtimeAnomalyDetectorsAndStart(SINGLE_ENTITY_DETECTOR, false);
                    createRealtimeAnomalyDetectorsAndStart(SINGLE_CATEGORY_HC_DETECTOR, false);
                    assertEquals(5, countDetectors(client(), null));
                    break;
                case UPGRADED:
                    Assert.assertTrue(pluginNames.contains("opensearch-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    verifyAnomalyDetector(TestHelpers.AD_BASE_DETECTORS_URI);
                    createRealtimeAnomalyDetectorsAndStart(SINGLE_ENTITY_DETECTOR, true);
                    createRealtimeAnomalyDetectorsAndStart(SINGLE_CATEGORY_HC_DETECTOR, true);
                    createRealtimeAnomalyDetectorsAndStart(MULTI_CATEGORY_HC_DETECTOR, true);
                    assertEquals(7, countDetectors(client(), null));
                    break;
            }
            break;
        }
    }

    @SuppressWarnings("unchecked")
    private void getTestData() throws IOException {
        Response searchResponse = TestHelpers
                .makeRequest(
                        client(),
                        "GET",
                        dataIndexName + "/_search",
                        null,
                        TestHelpers.toHttpEntity("{\"track_total_hits\": true}"),
                        ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
                );

        Map<String, Object> responseMap = entityAsMap(searchResponse);
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb111");
        System.out.println(responseMap);
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb222");
        System.out.println(responseMap.get("hits"));
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        //assertEquals(1, responseMap.get("hits"));
        Object total = ((Map<String, Object>) responseMap.get("hits")).get("total");
        assertEquals(totalDocs * categoryFieldSize * 2, ((Map<String, Object>) total).get("value"));
    }

    private String getUri() {
        switch (CLUSTER_TYPE) {
            case OLD:
                return "_nodes/" + CLUSTER_NAME + "-0/plugins";
            case MIXED:
                String round = System.getProperty("tests.rest.bwcsuite_round");
                if (round.equals("second")) {
                    return "_nodes/" + CLUSTER_NAME + "-1/plugins";
                } else if (round.equals("third")) {
                    return "_nodes/" + CLUSTER_NAME + "-2/plugins";
                } else {
                    return "_nodes/" + CLUSTER_NAME + "-0/plugins";
                }
            case UPGRADED:
                return "_nodes/plugins";
            default:
                throw new AssertionError("unknown cluster type: " + CLUSTER_TYPE);
        }
    }

    private List<String> createRealtimeAnomalyDetectorsAndStart(ADRestTestUtils.DetectorType detectorType, boolean newNode) throws Exception {
        switch (detectorType) {
            case SINGLE_ENTITY_DETECTOR:
                // Create single flow detector
                Response singleFLowDetectorResponse = createAnomalyDetector(client(), dataIndexName,
                        MockSimpleLog.TIME_FIELD,
                        detectionIntervalInMinutes, windowDelayIntervalInMinutes,
                        MockSimpleLog.VALUE_FIELD, aggregationMethod,
                        null, null);
                return startAnomalyDetector(singleFLowDetectorResponse, newNode);
            case SINGLE_CATEGORY_HC_DETECTOR:
                // Create single flow detector
                // Create single category detector
                Response singleCategoryHCDetectorResponse = createAnomalyDetector(client(), dataIndexName,
                        MockSimpleLog.TIME_FIELD,
                        detectionIntervalInMinutes, windowDelayIntervalInMinutes,
                        MockSimpleLog.VALUE_FIELD, aggregationMethod,
                        null, ImmutableList.of(MockSimpleLog.CATEGORY_FIELD));
                return startAnomalyDetector(singleCategoryHCDetectorResponse, newNode);
            case MULTI_CATEGORY_HC_DETECTOR:
                // Create single flow detector
                // Create single category detector
                Response multiCategoryHCDetectorResponse = createAnomalyDetector(client(), dataIndexName,
                        MockSimpleLog.TIME_FIELD,
                        detectionIntervalInMinutes, windowDelayIntervalInMinutes,
                        MockSimpleLog.VALUE_FIELD, aggregationMethod,
                        null, ImmutableList.of(MockSimpleLog.IP_FIELD, MockSimpleLog.CATEGORY_FIELD));
                return startAnomalyDetector(multiCategoryHCDetectorResponse, newNode);
            default:
                return null;
        }
    }

    private void createHistoricalAnomalyDetectorsAndStart() throws Exception {
        // only support single entity for historical detector
        Response historicalSingleFlowDetectorResponse = createAnomalyDetector(client(), dataIndexName,
                MockSimpleLog.TIME_FIELD,
                detectionIntervalInMinutes, windowDelayIntervalInMinutes,
                MockSimpleLog.VALUE_FIELD, aggregationMethod,
                null, null, true);
        List<String> historicalDetectorStartResult = startAnomalyDetector(historicalSingleFlowDetectorResponse, false);
        String detectorId = historicalDetectorStartResult.get(0);
        String taskId = historicalDetectorStartResult.get(1);
        waitUntilTaskDone(detectorId);
        List<ADTask> adTasks = searchLatestAdTaskOfDetector(client(), detectorId, ADTaskType.HISTORICAL.name());
        assertEquals(1, adTasks.size());
        assertEquals(taskId, adTasks.get(0).getTaskId());
        int adResultCount = countADResultOfDetector(client(), detectorId, taskId);
        assertTrue(adResultCount > 0);
        // waitUntilTaskDone(id);
//        Thread.sleep(1 * 60_000);
//        Response ceateDetectorResponse = null;
//        switch (detectorType) {
//            case SINGLE_ENTITY_DETECTOR:
//                // Create single flow detector
//                Response historicalSingleFlowDetectorResponse = createAnomalyDetector(client(), dataIndexName,
//                        MockSimpleLog.TIME_FIELD,
//                        detectionIntervalInMinutes, windowDelayIntervalInMinutes,
//                        MockSimpleLog.VALUE_FIELD, aggregationMethod,
//                        null, null, true);
//                List<String> historicalDetectorStartResult = startAnomalyDetector(historicalSingleFlowDetectorResponse, newNode);
//                String detectorId = historicalDetectorStartResult.get(0);
//                String taskId = historicalDetectorStartResult.get(1);
//                waitUntilTaskDone(detectorId);
//                ADTaskType taskType = newNode? ADTaskType.HISTORICAL_SINGLE_ENTITY : ADTaskType.HISTORICAL;
//                List<ADTask> adTasks = searchLatestAdTaskOfDetector(client(), detectorId, taskType.name());
//                assertEquals(1, adTasks.size());
//                assertEquals(taskId, adTasks.get(0).getTaskId());
//                // waitUntilTaskDone(id);
//                Thread.sleep(10 * 60_000);
//                break;
//            case SINGLE_CATEGORY_HC_DETECTOR:
//                if (newNode) {
//                    Response historicalSingleCategoryHCDetectorResponse = createAnomalyDetector(client(), dataIndexName,
//                            MockSimpleLog.TIME_FIELD,
//                            detectionIntervalInMinutes, windowDelayIntervalInMinutes,
//                            MockSimpleLog.VALUE_FIELD, aggregationMethod,
//                            null, null, true);
//                    List<String> singleCategoryHCStartResult = startAnomalyDetector(historicalSingleCategoryHCDetectorResponse, newNode);
//                    String singleCategoryHCDetectorId = singleCategoryHCStartResult.get(0);
//                    String singleCategoryHCTaskId = singleCategoryHCStartResult.get(1);
//                    waitUntilTaskDone(singleCategoryHCDetectorId);
//                    List<ADTask> singleCategoryHCAdTasks = searchLatestAdTaskOfDetector(client(), singleCategoryHCDetectorId, ADTaskType.HISTORICAL_HC_DETECTOR.name());
//                    assertEquals(1, singleCategoryHCAdTasks.size());
//                    assertEquals(singleCategoryHCTaskId, singleCategoryHCAdTasks.get(0).getTaskId());
//                }
//                break;
//            case MULTI_CATEGORY_HC_DETECTOR:
//                if (newNode) {
//                    Response historicalMultiCategoryHCDetectorResponse = createAnomalyDetector(client(), dataIndexName,
//                            MockSimpleLog.TIME_FIELD,
//                            detectionIntervalInMinutes, windowDelayIntervalInMinutes,
//                            MockSimpleLog.VALUE_FIELD, aggregationMethod,
//                            null, null, true);
//
//                }
//                break;
//            default:
//                break;
//        }
//        List<String> multiCategoryHCStartResult = startAnomalyDetector(historicalMultiCategoryHCDetectorResponse, newNode);
//        String multiCategoryHCDetectorId = multiCategoryHCStartResult.get(0);
//        String multiCategoryHCTaskId = multiCategoryHCStartResult.get(1);
//        waitUntilTaskDone(multiCategoryHCDetectorId);
//        List<ADTask> multiCategoryHCAdTasks = searchLatestAdTaskOfDetector(client(), multiCategoryHCDetectorId, ADTaskType.HISTORICAL_HC_DETECTOR.name());
//        assertEquals(1, multiCategoryHCAdTasks.size());
//        assertEquals(multiCategoryHCTaskId, multiCategoryHCAdTasks.get(0).getTaskId());
    }

    private List<String> startAnomalyDetector(Response response, boolean newNode) throws IOException {
        // verify that the detector is created
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        int version = (int) responseMap.get("_version");
        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
        assertTrue("incorrect version", version > 0);

        Response startDetectorResponse = TestHelpers
                .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + id + "/_start",
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        null
                );
        Map<String, Object> startDetectorResponseMap = responseAsMap(startDetectorResponse);
        String taskId = (String) startDetectorResponseMap.get("_id");
        assertNotNull(taskId);

        return ImmutableList.of(id, taskId);
    }

//    private void createHistoricalAnomalyDetector() throws Exception {
//        AnomalyDetector detector1 = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
//        //String indexName = detector.getIndices().get(0);
//        String testFeature = "testFeature";
//        XContentParser parser = parser( "{\"" + testFeature + "\":{\"sum\":{\"field\":\"" + MockSimpleLog.VALUE_FIELD + "\"}}}");
//        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
//        AggregationBuilder aggregationBuilder = parsed.getAggregatorFactories().iterator().next();
//        AnomalyDetector detector = new AnomalyDetector(detector1.getDetectorId(), detector1.getVersion(), detector1.getName(),
//                detector1.getDescription(),
//                MockSimpleLog.TIME_FIELD,
//                ImmutableList.of(dataIndexName),
//                ImmutableList.of(new Feature("aaaaa", testFeature, true, aggregationBuilder)),
//                detector1.getFilterQuery(),
//                new IntervalTimeConfiguration(1, ChronoUnit.MINUTES),
//                detector1.getWindowDelay(), detector1.getShingleSize(), null,
//                detector1.getSchemaVersion(), detector1.getLastUpdateTime(),
//                null, null);
//        Instant now = Instant.now();
//
//        DetectionDateRange dateRange = new DetectionDateRange(now.minus(10, ChronoUnit.DAYS), now);
//        detector.setDetectionDateRange(dateRange);
//        //String indexName = detector.getIndices().get(0);
//        //TestHelpers.createIndex(client(), dataIndexName, TestHelpers.toHttpEntity("{\"name\": \"test\"}"));
//
//        Response response = TestHelpers
//            .makeRequest(
//                client(),
//                "POST",
//                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI,
//                ImmutableMap.of(),
//                TestHelpers.toHttpEntity(detector),
//                null
//            );
//        // verify that the detector is created
//        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
//        Map<String, Object> responseMap = entityAsMap(response);
//        String id = (String) responseMap.get("_id");
//        int version = (int) responseMap.get("_version");
//        logger.info("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + id);
//        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
//        assertTrue("incorrect version", version > 0);
//        Response startDetectorResponse = TestHelpers
//                .makeRequest(
//                        client(),
//                        "POST",
//                        TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + id + "/_start",
//                        ImmutableMap.of(),
//                        dateRange == null ? null : TestHelpers.toHttpEntity(dateRange),
//                        null
//                );
//
//        Map<String, Object> startDetectorResponseMap = responseAsMap(startDetectorResponse);
//        String taskId = (String) startDetectorResponseMap.get("_id");
//        assertNotNull(taskId);
//
//        // waitUntilTaskDone(id);
//        Thread.sleep(60_000);
//
//    }

    protected ADTaskProfile waitUntilTaskDone(String detectorId) throws InterruptedException {
        return waitUntilTaskReachState(detectorId, TestHelpers.HISTORICAL_ANALYSIS_DONE_STATS);
    }

    protected ADTaskProfile waitUntilTaskReachState(String detectorId, Set<String> targetStates) throws InterruptedException {
        int i = 0;
        ADTaskProfile adTaskProfile = null;
        while ((adTaskProfile == null || !targetStates.contains(adTaskProfile.getAdTask().getState())) && i < 60) {
            try {
                adTaskProfile = getADTaskProfile(detectorId);
                System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc");
                System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc");
                System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc");
                System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc");
                System.out.println(adTaskProfile);
                System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc");
                System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc");
                System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                Thread.sleep(1000);
            }
            i++;
        }
        assertNotNull(adTaskProfile);
        return adTaskProfile;
    }

    protected ADTaskProfile getADTaskProfile(String detectorId) throws IOException {
        Response profileResponse = TestHelpers
                .makeRequest(
                        client(),
                        "GET",
                        TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + detectorId + "/_profile?_all",
                        ImmutableMap.of(),
                        "",
                        null
                );
        return parseADTaskProfile(profileResponse);
    }

    private ADTaskProfile parseADTaskProfile(Response profileResponse) throws IOException {
        String profileResult = EntityUtils.toString(profileResponse.getEntity());
        XContentParser parser = TestHelpers.parser(profileResult);
        ADTaskProfile adTaskProfile = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc1111, " + fieldName);
            parser.nextToken();
            if ("ad_task".equals(fieldName)) {
                adTaskProfile = ADTaskProfile.parse(parser);
                System.out.println("cccccccccccccccccccccccccccccccccccccccccccccccccc222222, " + fieldName);
            } else {
                parser.skipChildren();
            }
        }
        return adTaskProfile;
    }

    private void verifyAnomalyDetector(String uri) throws Exception {
        Response response = TestHelpers.makeRequest(client(), "GET", uri + "/" + RestHandlerUtils.COUNT, null, "", null);
        Map<String, Object> responseMap = entityAsMap(response);
        Integer count = (Integer) responseMap.get("count");
        assertEquals(3, (long) count);
    }

}
