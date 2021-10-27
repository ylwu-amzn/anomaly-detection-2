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

package org.opensearch.ad.rest;

import static org.opensearch.ad.rest.RestSearchAnomalyResultAction.SEARCH_ANOMALY_RESULT_ACTION;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_DETECTOR_UPPER_LIMIT;
import static org.opensearch.ad.util.RestHandlerUtils.RESULT_INDEX;
import static org.opensearch.ad.util.RestHandlerUtils.getSourceContext;
import static org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Abstract class to handle search request.
 */
public abstract class AbstractSearchAction<T extends ToXContentObject> extends BaseRestHandler {

    private final String index;
    private final Class<T> clazz;
    private final List<String> urlPaths;
    private final List<Pair<String, String>> deprecatedPaths;
    private final ActionType<SearchResponse> actionType;

    private final Logger logger = LogManager.getLogger(AbstractSearchAction.class);

    public AbstractSearchAction(
        List<String> urlPaths,
        List<Pair<String, String>> deprecatedPaths,
        String index,
        Class<T> clazz,
        ActionType<SearchResponse> actionType
    ) {
        this.index = index;
        this.clazz = clazz;
        this.urlPaths = urlPaths;
        this.deprecatedPaths = deprecatedPaths;
        this.actionType = actionType;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        logger.info("+++++++++++1111111111122222222222222 ffffffffff");
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        searchSourceBuilder.fetchSource(getSourceContext(request));
        searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(this.index);

        String resultIndex = SEARCH_ANOMALY_RESULT_ACTION.equals(getName()) ? Strings.trimToNull(request.param(RESULT_INDEX)) : null;
        return channel -> {
            if (resultIndex == null) {
                client.execute(actionType, searchRequest, search(channel));
                return;
            }
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                client.admin().indices().prepareGetIndex().setIndices(resultIndex).execute(ActionListener.wrap(getIndexResponse -> {
                    String[] indices = getIndexResponse.getIndices();
                    logger.info("+++++++++++ all indices is :  {}", Arrays.toString(indices));
                    if (indices == null || indices.length == 0) {
                        try {
                            channel.sendResponse(new BytesRestResponse(channel, new ResourceNotFoundException("Can't find inex")));
                        } catch (Exception exception) {
                            logger.error("Failed to send back failure response for search AD result", exception);
                        }
                        return;
                    }
                    SearchSourceBuilder searchResultIndexBuilder = new SearchSourceBuilder();
                    AggregationBuilder aggregation = new TermsAggregationBuilder("result_index")
                            .field(AnomalyDetector.RESULT_INDEX_FIELD)
                            .size(MAX_DETECTOR_UPPER_LIMIT);
                    searchResultIndexBuilder.aggregation(aggregation).size(0);
                    SearchRequest searchResultIndex = new SearchRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).source(searchResultIndexBuilder);

                    logger.info("+++++++++++ start to search result indices of all detector");

                    // Search result indices of all detectors
                    logger.info("++++++++++++++++ stash context");
                    client.search(searchResultIndex, ActionListener.wrap(allResultIndicesResponse -> {
                        context.restore();
                        logger.info("++++++++++++++++ restore context");
                        Aggregations aggregations = allResultIndicesResponse.getAggregations();
                        StringTerms resultIndicesAgg = aggregations.get("result_index");
                        List<StringTerms.Bucket> buckets = resultIndicesAgg.getBuckets();
                        Set<String> resultIndicesOfDetector = new HashSet<>();
                        if (buckets != null) {
                            buckets.stream().forEach(b -> resultIndicesOfDetector.add(b.getKeyAsString()));
                        }
                        logger.info(aggregations);
                        List<String> targetIndices = new ArrayList<>();
                        for (String index : indices) {
                            if (resultIndicesOfDetector.contains(index)) {
                                targetIndices.add(index);
                            }
                        }
                        logger.info("+++++++++++++++++++++ targetIndices: {}", Arrays.toString(targetIndices.toArray(new String[0])));
                        if (targetIndices.size() == 0) {
                            //no result indices used by detectors, just search default result index
                            client.execute(actionType, searchRequest, search(channel));
                            return;
                        }

                        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
                        for (String index : targetIndices) {
                            multiSearchRequest.add(new SearchRequest(index).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(0)));
                        }
                        List<String> readableIndices = new ArrayList<>();
                        client.multiSearch(multiSearchRequest, ActionListener.wrap(multiSearchResponse -> {
                            MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
                            for (int i = 0; i < responses.length; i++) {
                                MultiSearchResponse.Item item = responses[i];
                                String indexName = targetIndices.get(i);
                                if (item.getFailure() == null) {
                                    readableIndices.add(indexName);
                                } else {
                                    logger.warn("ylwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww {}, {}", indexName, item.getFailureMessage());
                                }
                            }
                            logger.info("++++++++++0000000000 all readable indices : {}", Arrays.toString(readableIndices.toArray(new String[0])));

                            // search anomaly results for all readable result indices and default result index
                            readableIndices.add(this.index);
                            executeWithAdmin(client, () -> {
                                searchRequest.indices(readableIndices.toArray(new String[0]));
                                client.search(searchRequest, search(channel));
                            }, channel);
//                        SearchRequest searchResultIndexRequest = new SearchRequest()
//                                .indices(readableIndices.toArray(new String[0]))
//                                .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(0));
//                        client.search(searchResultIndexRequest, ActionListener.wrap(r -> {
//                            executeWithAdmin(client, () -> {
//                                searchRequest.indices(resultIndex, this.index);
//                                client.search(searchRequest, search(channel));
//                            }, channel);
//                        }, e -> {
//                            if (e instanceof OpenSearchSecurityException) {
//                                logger.warn("No permission to search AD result index [{}], will query default AD result index only", resultIndex);
//                                executeWithAdmin(client, () -> { client.search(searchRequest, search(channel)); }, channel);
//                            } else {
//                                logger.error(e);
//                                try {
//                                    channel.sendResponse(new BytesRestResponse(channel, e));
//                                } catch (Exception exception) {
//                                    logger.error("Failed to send back failure response for search AD result", exception);
//                                }
//                            }
//                        }));
                        }, multiSearchException -> {
                            logger.error("Failed to search multiple indices", multiSearchException);
                            try {
                                channel.sendResponse(new BytesRestResponse(channel, multiSearchException));
                            } catch (Exception exception) {
                                logger.error("Failed to send back failure response for search AD result", exception);
                            }
                        }));

                    }, ex -> {
                        logger.error("Failed to search result indices for all detectors", ex);
                        try {
                            channel.sendResponse(new BytesRestResponse(channel, ex));
                        } catch (Exception exception) {
                            logger.error("Failed to send back failure response for search AD result", exception);
                        }
                    }));

                }, getIndexException -> {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, getIndexException));
                    } catch (Exception exception) {
                        logger.error("Failed to send back failure response for search AD result", exception);
                    }
                }));
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, new ResourceNotFoundException("Can't find inex")));
                } catch (Exception exception) {
                    logger.error("Failed to send back failure response for search AD result", exception);
                }
            }
        };
    }

    private void executeWithAdmin(NodeClient client, AnomalyDetectorFunction function, RestChannel channel) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            function.execute();
        } catch (Exception e) {
            logger.error(e);
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (Exception exception) {
                logger.error("Failed to send back failure response for search AD result", exception);
            }
        }
    }

    private RestResponseListener<SearchResponse> search(RestChannel channel) {
        return new RestResponseListener<SearchResponse>(channel) {
            @Override
            public RestResponse buildResponse(SearchResponse response) throws Exception {
                if (response.isTimedOut()) {
                    return new BytesRestResponse(RestStatus.REQUEST_TIMEOUT, response.toString());
                }

                if (clazz == AnomalyDetector.class) {
                    for (SearchHit hit : response.getHits()) {
                        XContentParser parser = XContentType.JSON
                            .xContent()
                            .createParser(
                                channel.request().getXContentRegistry(),
                                LoggingDeprecationHandler.INSTANCE,
                                hit.getSourceAsString()
                            );
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

                        // write back id and version to anomaly detector object
                        ToXContentObject xContentObject = AnomalyDetector.parse(parser, hit.getId(), hit.getVersion());
                        XContentBuilder builder = xContentObject.toXContent(jsonBuilder(), EMPTY_PARAMS);
                        hit.sourceRef(BytesReference.bytes(builder));
                    }
                }

                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), EMPTY_PARAMS));
            }
        };
    }

    @Override
    public List<Route> routes() {
        List<Route> routes = new ArrayList<>();
        for (String path : urlPaths) {
            routes.add(new Route(RestRequest.Method.POST, path));
            routes.add(new Route(RestRequest.Method.GET, path));
        }
        return routes;
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        List<ReplacedRoute> replacedRoutes = new ArrayList<>();
        for (Pair<String, String> deprecatedPath : deprecatedPaths) {
            replacedRoutes
                .add(
                    new ReplacedRoute(RestRequest.Method.POST, deprecatedPath.getKey(), RestRequest.Method.POST, deprecatedPath.getValue())
                );
            replacedRoutes
                .add(new ReplacedRoute(RestRequest.Method.GET, deprecatedPath.getKey(), RestRequest.Method.GET, deprecatedPath.getValue()));

        }
        return replacedRoutes;
    }
}
