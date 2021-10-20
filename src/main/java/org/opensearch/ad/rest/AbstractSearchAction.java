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

import static org.opensearch.ad.util.RestHandlerUtils.RESULT_INDEX;
import static org.opensearch.ad.util.RestHandlerUtils.getSourceContext;
import static org.opensearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.search.SearchHit;
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
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        searchSourceBuilder.fetchSource(getSourceContext(request));
        searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);

        String resultIndex = request.param(RESULT_INDEX);
        logger.info("ylwudebug ++++++++++++++++++++ result index : {}", resultIndex);

        logger.info("ylwudebug ---------- searchSourceBuilder : {}", searchSourceBuilder);
        QueryBuilder query = searchSourceBuilder.query();

        if (query != null && query instanceof BoolQueryBuilder) {
            logger.info("ylwudebug ----------++++++++++ BoolQuery : {}, query class: {}", query, query.getClass());
        }
        if (query != null) {
            logger.info("ylwudebug ---------- query : {}, query class: {}", query, query.getClass());
        } else {
            logger.info("ylwudebug ---------- query is null");
        }

        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(this.index);

        return channel -> {
            if (resultIndex == null) {
                client.execute(actionType, searchRequest, search(channel));
                return;
            }
            SearchSourceBuilder matchAll = new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(0);
            // If user specify result index, we also query default system index to get old AD result.
            SearchRequest searchCustomIndexRequest = new SearchRequest().source(matchAll).indices(index, resultIndex);
            client.search(searchCustomIndexRequest, ActionListener.wrap(r -> {
                logger.info("ylwudebug ---------- search result index {}", resultIndex);
                searchRequest.indices(resultIndex);
                client.execute(actionType, searchRequest, search(channel));
            }, e-> {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException exception) {
                    logger.error("Failed to send back search exception result for " + actionType, exception);
                }
            }));
        };


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
