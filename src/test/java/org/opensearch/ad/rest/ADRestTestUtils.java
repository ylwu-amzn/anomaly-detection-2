/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;
import static org.opensearch.test.OpenSearchTestCase.randomDoubleBetween;
import static org.opensearch.test.OpenSearchTestCase.randomInt;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap;

public class ADRestTestUtils {
    public enum DetectorType {
        SINGLE_ENTITY_DETECTOR,
        SINGLE_CATEGORY_HC_DETECTOR,
        MULTI_CATEGORY_HC_DETECTOR
    }

    public static Response ingestSimpleMockLog(
            RestClient client,
            String indexName,
            int startDays,
            int totalDoc,
            long intervalInMinutes,
            ToDoubleFunction<Integer> valueFunc,
            int ipSize,
            int categorySize,
            boolean createIndex
    ) throws IOException {
        if (createIndex) {
            TestHelpers
                    .makeRequest(
                            client,
                            "PUT",
                            indexName,
                            null,
                            TestHelpers.toHttpEntity(MockSimpleLog.INDEX_MAPPING),
                            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "OpenSearch"))
                    );
        }

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
                        client,
                        "POST",
                        "_bulk?refresh=true",
                        null,
                        TestHelpers.toHttpEntity(bulkRequestBuilder.toString()),
                        ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
                );
        return bulkResponse;
    }

    public static Response ingestTestDataForHistoricalAnalysis(RestClient client, String indexName, int detectionIntervalInMinutes,
                                                           boolean createIndex, int startDays, int totalDocs,
                                                           int categoryFieldSize) throws IOException {
        return ingestSimpleMockLog(client, indexName, startDays, totalDocs, detectionIntervalInMinutes, (i) -> {
            if (i % 500 == 0) {
                return randomDoubleBetween(100, 1000, true);
            } else {
                return randomDoubleBetween(1, 10, true);
            }
        }, categoryFieldSize, categoryFieldSize, createIndex);
    }

    public static Response createAnomalyDetector(RestClient client, String indexName, String timeField, int detectionIntervalInMinutes, int windowDelayIntervalInMinutes, String valueField, String aggregationMethod, String filterQuery, List<String> categoryFields) throws Exception {
        return createAnomalyDetector(client, indexName, timeField, detectionIntervalInMinutes, windowDelayIntervalInMinutes,
                valueField, aggregationMethod, filterQuery, categoryFields, false);
    }
    public static Response createAnomalyDetector(RestClient client, String indexName, String timeField, int detectionIntervalInMinutes,
                                          int windowDelayIntervalInMinutes, String valueField,
                                                 String aggregationMethod, String filterQuery,
                                                 List<String> categoryFields, boolean historical) throws Exception {
        Instant now = Instant.now();
        AnomalyDetector detector = new AnomalyDetector(
                randomAlphaOfLength(10),
                randomLong(),
                randomAlphaOfLength(20),
                randomAlphaOfLength(30),
                timeField,
                ImmutableList.of(indexName),
                ImmutableList.of(TestHelpers.randomFeature(randomAlphaOfLength(5), valueField, aggregationMethod, true)),
                filterQuery == null ? TestHelpers.randomQuery("{\"match_all\":{\"boost\":1}}") : TestHelpers.randomQuery(filterQuery),
                new IntervalTimeConfiguration(detectionIntervalInMinutes, ChronoUnit.MINUTES),
                new IntervalTimeConfiguration(windowDelayIntervalInMinutes, ChronoUnit.MINUTES),
                randomIntBetween(1, 20),
                null,
                randomInt(),
                now,
                categoryFields,
                TestHelpers.randomUser()
        );

        if (historical) {
            detector.setDetectionDateRange(new DetectionDateRange(now.minus(30, ChronoUnit.DAYS), now));
        }

        return TestHelpers
                .makeRequest(
                        client,
                        "POST",
                        TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI,
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(detector),
                        null
                );
        // verify that the detector is created
//        assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
//        Map<String, Object> responseMap = entityAsMap(response);
//        String id = (String) responseMap.get("_id");
//        int version = (int) responseMap.get("_version");
//        assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, id);
//        assertTrue("incorrect version", version > 0);
    }

    @SuppressWarnings("unchecked")
    public static List<ADTask> searchLatestAdTaskOfDetector(RestClient client, String detectorId, String taskType) throws IOException {
        List<ADTask> adTasks = new ArrayList<>();
        Response searchAdTaskResponse = TestHelpers
                .makeRequest(
                        client,
                        "POST",
                        TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/tasks/_search",
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity("{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"detector_id\":\""+detectorId+"\"}},{\"term\":{\"is_latest\":\"true\"}},{\"terms\":{\"task_type\":[\""+taskType+"\"]}}]}},\"sort\":[{\"execution_start_time\":{\"order\":\"desc\"}}],\"size\":100}"),
                        null
                );
        Map<String, Object> responseMap = entityAsMap(searchAdTaskResponse);
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        Object totalHits = hits.get("total");
        Integer totalTasks = (Integer)((Map<String, Object>) totalHits).get("value");

        if (totalTasks == 0) {
            return adTasks;
        }
        List<Object> adTaskResponses = (List<Object>) hits.get("hits");
        for (Object adTaskResponse : adTaskResponses) {
            String id = (String)((Map<String, Object>) adTaskResponse).get("_id");
            Map<String, Object> source = (Map<String, Object>)((Map<String, Object>) adTaskResponse).get("_source");
            String state = (String) source.get(ADTask.STATE_FIELD);
            String parsedDetectorId = (String) source.get(ADTask.DETECTOR_ID_FIELD);
            Double taskProgress = (Double) source.get(ADTask.TASK_PROGRESS_FIELD);
            Double initProgress = (Double) source.get(ADTask.INIT_PROGRESS_FIELD);
            String parsedTaskType = (String) source.get(ADTask.TASK_TYPE_FIELD);
            String coordinatingNode = (String) source.get(ADTask.COORDINATING_NODE_FIELD);
            ADTask adTask = ADTask.builder().taskId(id).state(state).detectorId(parsedDetectorId).taskProgress(taskProgress.floatValue()).initProgress(initProgress.floatValue())
                    .taskType(parsedTaskType).coordinatingNode(coordinatingNode)
                    .build();
            adTasks.add(adTask);
        }
        return  adTasks;
    }

    @SuppressWarnings("unchecked")
    public static int countADResultOfDetector(RestClient client, String detectorId, String taskId) throws IOException {
        String taskFilter = "TASK_FILTER";
        String query = "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"detector_id\":\""+detectorId+"\"}}" + taskFilter +"]}},\"track_total_hits\":true,\"size\":0}";
        if (taskId != null) {
            query = query.replace(taskFilter, ",{\"term\":{\"task_id\":\""+taskId+"\"}}");
        } else {
            query = query.replace(taskFilter, "");
        }
        System.out.println("ddddddddddddd111111, " + query);
        Response searchAdTaskResponse = TestHelpers
                .makeRequest(
                        client,
                        "GET",
                        TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/results/_search",
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(query),

                        null
                );
        Map<String, Object> responseMap = entityAsMap(searchAdTaskResponse);
        Map<String, Object> hits = (Map<String, Object>) ((Map<String, Object>) responseMap.get("hits")).get("total");
        return (int) hits.get("value");
    }

    @SuppressWarnings("unchecked")
    public static int countDetectors(RestClient client, String detectorType) throws IOException {
        String detectorTypeFilter = "DETECTOR_TYPE_FILTER";
        String query = "{\"query\":{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"name\"}}" + detectorTypeFilter + "]}},\"track_total_hits\":true,\"size\":0}";
        if (detectorType != null) {
            query = query.replace(detectorTypeFilter, ",{\"term\":{\"detector_type\":\""+detectorType+"\"}}");
        } else {
            query = query.replace(detectorTypeFilter, "");
        }
        Response searchAdTaskResponse = TestHelpers
                .makeRequest(
                        client,
                        "GET",
                        TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/_search",
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(query),

                        null
                );
        Map<String, Object> responseMap = entityAsMap(searchAdTaskResponse);
        Map<String, Object> hits = (Map<String, Object>) ((Map<String, Object>) responseMap.get("hits")).get("total");
        return (int) hits.get("value");
    }
}
