/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.ad.bwc;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import com.google.common.collect.ImmutableMap;

import static org.opensearch.ad.TestHelpers.parser;

public class ADBackwardsCompatibilityIT extends OpenSearchRestTestCase {

    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");
    private String dataIndexName = "test_data_for_ad_plugin";

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

    private Response ingestSimpleMockLog(
            String indexName,
            int startDays,
            int totalDoc,
            long intervalInMinutes,
            ToDoubleFunction<Integer> valueFunc,
            int ipSize,
            int categorySize
    ) throws IOException {
        TestHelpers
                .makeRequest(
                        client(),
                        "PUT",
                        indexName,
                        null,
                        TestHelpers.toHttpEntity(MockSimpleLog.INDEX_MAPPING),
                        ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
                );

        Response statsResponse = TestHelpers.makeRequest(client(), "GET", indexName, ImmutableMap.of(), "", null);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(statsResponse));
        String result = EntityUtils.toString(statsResponse.getEntity());
        assertTrue(result.contains(indexName));

        StringBuilder bulkRequestBuilder = new StringBuilder();
        Instant startTime = Instant.now().minus(startDays, ChronoUnit.DAYS);
        for (int i = 0; i < totalDoc; i++) {
            for (int m = 0; m < ipSize; m++) {
                String ip = "192.168.1." + m;
                for (int n = 0; n < categorySize; n++) {
                    String category = "category" + n;
                    String docId = randomAlphaOfLength(10);
                    bulkRequestBuilder.append("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"" + docId + "\" } }\n");
                    MockSimpleLog simpleLog1 = new MockSimpleLog(
                            startTime,
                            valueFunc.applyAsDouble(i),
                            ip,
                            category,
                            randomBoolean(),
                            randomAlphaOfLength(5)
                    );
                    bulkRequestBuilder.append(TestHelpers.toJsonString(simpleLog1));
                    bulkRequestBuilder.append("\n");
                }
            }
            startTime = startTime.plus(intervalInMinutes, ChronoUnit.MINUTES);
        }
        Response bulkResponse = TestHelpers
                .makeRequest(
                        client(),
                        "POST",
                        "_bulk?refresh=true",
                        null,
                        TestHelpers.toHttpEntity(bulkRequestBuilder.toString()),
                        ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
                );
        return bulkResponse;
    }

    private void ingestTestDataForHistoricalAnalysis(String indexName, int detectionIntervalInMinutes) throws IOException {
        ingestSimpleMockLog(indexName, 5, 6000, detectionIntervalInMinutes, (i) -> {
            if (i % 500 == 0) {
                return randomDoubleBetween(100, 1000, true);
            } else {
                return randomDoubleBetween(1, 10, true);
            }
        }, 2, 2);
    }

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
                    ingestTestDataForHistoricalAnalysis(dataIndexName, 1);
                    Assert.assertTrue(pluginNames.contains("opendistro-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opendistro-job-scheduler"));
                    createBasicAnomalyDetector();
                    break;
                case MIXED:
                    Assert.assertTrue(pluginNames.contains("opensearch-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    verifyAnomalyDetector(TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI);
                    createBasicAnomalyDetector();
                    break;
                case UPGRADED:
                    Assert.assertTrue(pluginNames.contains("opensearch-anomaly-detection"));
                    Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
                    verifyAnomalyDetector(TestHelpers.AD_BASE_DETECTORS_URI);
                    break;
            }
            break;
        }
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

    private void createBasicAnomalyDetector() throws Exception {
        AnomalyDetector detector1 = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null);
        //String indexName = detector.getIndices().get(0);
        String testFeature = "testFeature";
        XContentParser parser = parser( "{\"" + testFeature + "\":{\"sum\":{\"field\":\"" + MockSimpleLog.VALUE_FIELD + "\"}}}");
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        AggregationBuilder aggregationBuilder = parsed.getAggregatorFactories().iterator().next();
        AnomalyDetector detector = new AnomalyDetector(detector1.getDetectorId(), detector1.getVersion(), detector1.getName(),
                detector1.getDescription(),
                MockSimpleLog.TIME_FIELD,
                ImmutableList.of(dataIndexName),
                ImmutableList.of(new Feature("aaaaa", testFeature, true, aggregationBuilder)),
                detector1.getFilterQuery(),
                new IntervalTimeConfiguration(1, ChronoUnit.MINUTES),
                detector1.getWindowDelay(), detector1.getShingleSize(), null,
                detector1.getSchemaVersion(), detector1.getLastUpdateTime(),
                null, null);
        Instant now = Instant.now();

        DetectionDateRange dateRange = new DetectionDateRange(now.minus(10, ChronoUnit.DAYS), now);
        detector.setDetectionDateRange(dateRange);
        //String indexName = detector.getIndices().get(0);
        //TestHelpers.createIndex(client(), dataIndexName, TestHelpers.toHttpEntity("{\"name\": \"test\"}"));

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
        // verify that the detector is created
        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String id = (String) responseMap.get("_id");
        int version = (int) responseMap.get("_version");
        logger.info("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + id);
        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
        assertTrue("incorrect version", version > 0);
        Response startDetectorResponse = TestHelpers
                .makeRequest(
                        client(),
                        "POST",
                        TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + id + "/_start",
                        ImmutableMap.of(),
                        dateRange == null ? null : TestHelpers.toHttpEntity(dateRange),
                        null
                );

        Map<String, Object> startDetectorResponseMap = responseAsMap(startDetectorResponse);
        String taskId = (String) startDetectorResponseMap.get("_id");
        assertNotNull(taskId);

        // waitUntilTaskDone(id);
        Thread.sleep(60_000);

    }

    protected ADTaskProfile waitUntilTaskDone(String detectorId) throws InterruptedException {
        return waitUntilTaskReachState(detectorId, TestHelpers.HISTORICAL_ANALYSIS_DONE_STATS);
    }

    protected ADTaskProfile waitUntilTaskReachState(String detectorId, Set<String> targetStates) throws InterruptedException {
        int i = 0;
        ADTaskProfile adTaskProfile = null;
        while ((adTaskProfile == null || !targetStates.contains(adTaskProfile.getAdTask().getState())) && i < 60) {
            try {
                adTaskProfile = getADTaskProfile(detectorId);
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
                        TestHelpers.HISTORICAL_ANALYSIS_DONE_STATS + "/" + detectorId + "/_profile/ad_task",
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
            parser.nextToken();
            if ("ad_task".equals(fieldName)) {
                adTaskProfile = ADTaskProfile.parse(parser);
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
        assertEquals(1, (long) count);
    }

}
