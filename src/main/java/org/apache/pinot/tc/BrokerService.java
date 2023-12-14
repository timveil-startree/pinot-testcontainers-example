package org.apache.pinot.tc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.tc.api.QueryException;
import org.apache.pinot.tc.api.QueryResponse;
import org.apache.pinot.tc.api.SqlQuery;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class BrokerService {

    private final WebClient client;
    private final ObjectMapper objectMapper;

    public BrokerService(@Qualifier("broker_client") WebClient client, ObjectMapper objectMapper) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    public QueryResponse executeQuery(String query) throws JsonProcessingException {
        return executeMultiStageQuery(query);
    }

    public QueryResponse executeMultiStageQuery(String query) throws JsonProcessingException {
        return getQueryResponse(query, "query");
    }

    public QueryResponse executeSingleStageQuery(String query) throws JsonProcessingException {
        return getQueryResponse(query, "query/sql");
    }

    private QueryResponse getQueryResponse(String query, String path) throws JsonProcessingException {
        QueryResponse response = client.post()
                .uri(uriBuilder -> uriBuilder.path(path).build())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(objectMapper.writeValueAsString(new SqlQuery(query)))
                .retrieve()
                .bodyToMono(QueryResponse.class)
                .block();

        if (response != null && response.getExceptions() != null && !response.getExceptions().isEmpty()) {
            for (QueryException ex : response.getExceptions()) {
                throw new RuntimeException(ex.getMessage());
            }
        }

        return response;
    }
}
