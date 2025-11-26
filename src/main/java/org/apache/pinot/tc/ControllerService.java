package org.apache.pinot.tc;

import org.apache.pinot.tc.api.PostResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.Charset;

@Service
public class ControllerService {

    private static final Logger log = LoggerFactory.getLogger(ControllerService.class);

    private final WebClient client;
    private final JsonMapper jsonMapper;
    private final Environment environment;

    public ControllerService(@Qualifier("controller_client") WebClient client, JsonMapper jsonMapper, Environment environment) {
        this.client = client;
        this.jsonMapper = jsonMapper;
        this.environment = environment;
    }

    public PostResponse createSchema(Resource resource) throws IOException, JacksonException {
        String schemaConfig = resource.getContentAsString(Charset.defaultCharset());
        return createSchema(schemaConfig);
    }

    public PostResponse createSchema(String schemaConfig) throws JacksonException {

        String response = client.post()
                .uri(uriBuilder -> uriBuilder.path("schemas").build())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(schemaConfig)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        log.debug("raw create schema response: \n\n{}\n", response);

        return jsonMapper.readValue(response, PostResponse.class);

    }

    public PostResponse createTable(Resource resource) throws IOException, JacksonException {
        String tableConfig = resource.getContentAsString(Charset.defaultCharset());
        return createTable(tableConfig);
    }

    public PostResponse createTable(String tableConfig) throws JacksonException {

        String response = client.post()
                .uri(uriBuilder -> uriBuilder.path("tables").build())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(tableConfig)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        log.debug("raw create table response: \n\n{}\n", response);

        return jsonMapper.readValue(response, PostResponse.class);
    }

    public String scheduleTask(String taskName, String tableName) throws JacksonException {

        String response = client.post()
                .uri(uriBuilder -> uriBuilder.path("tasks/schedule").queryParam("taskType", taskName).queryParam("tableName", tableName).build())
                .contentType(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        log.debug("raw schedule task response: \n\n{}\n", response);

        JsonNode jsonNode = jsonMapper.readTree(response);

        return jsonNode.get(taskName).asText();

    }

    public PostResponse ingestFromFile(String tableName, BatchIngestConfiguration configuration, Resource resource) throws IOException, JacksonException {

        String configurationAsString = jsonMapper.writeValueAsString(configuration);

        log.debug("batch configuration as string: {}", configurationAsString);

        MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();
        bodyBuilder.part("file", resource);

        UriComponents uriComponents = UriComponentsBuilder.fromUriString(environment.getProperty("pinot.controller.url") + "/ingestFromFile")
                .queryParam("tableNameWithType", tableName)
                .queryParam("batchConfigMapStr", configurationAsString)
                .build();

        log.debug("uri: {}", uriComponents);

        String response = client.post()
                .uri(uriComponents.toUri())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                .retrieve()
                .bodyToMono(String.class)
                .block();

        log.debug("raw ingest from file response: \n\n{}\n", response);

        return jsonMapper.readValue(response, PostResponse.class);

    }
}
