package org.apache.pinot.tc;

import org.apache.pinot.tc.api.QueryException;
import org.apache.pinot.tc.api.QueryResponse;
import org.apache.pinot.tc.api.SqlQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Service
public class BrokerService {

    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);

    private final WebClient client;
    private final JsonMapper jsonMapper;

    public BrokerService(@Qualifier("broker_client") WebClient client, JsonMapper jsonMapper) {
        this.client = client;
        this.jsonMapper = jsonMapper;
    }

    public QueryResponse executeQuery(String query) throws JacksonException {
        return executeMultiStageQuery(query);
    }

    public QueryResponse executeMultiStageQuery(String query) throws JacksonException {
        return getQueryResponse(query, "query");
    }

    public QueryResponse executeSingleStageQuery(String query) throws JacksonException {
        return getQueryResponse(query, "query/sql");
    }

    private QueryResponse getQueryResponse(String query, String path) throws JacksonException {
        String response = client.post()
                .uri(uriBuilder -> uriBuilder.path(path).build())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonMapper.writeValueAsString(new SqlQuery(query)))
                .retrieve()
                .bodyToMono(String.class)
                .block();

        log.debug("raw query results: \n\n{}\n", response);

        QueryResponse queryResponse = jsonMapper.readValue(response, QueryResponse.class);

        if (queryResponse != null && queryResponse.getExceptions() != null && !queryResponse.getExceptions().isEmpty()) {
            for (QueryException ex : queryResponse.getExceptions()) {
                log.error(ex.getMessage(), ex);
            }
        }

        return queryResponse;
    }
}
