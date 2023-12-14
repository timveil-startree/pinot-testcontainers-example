package org.apache.pinot.tc;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.IOException;
import java.nio.charset.Charset;

@Service
public class ControllerService {

    private static final Logger log = LoggerFactory.getLogger(ControllerService.class);

    private final WebClient client;
    private final ObjectMapper objectMapper;
    private final Environment environment;

    public ControllerService(@Qualifier("controller_client") WebClient client, ObjectMapper objectMapper, Environment environment) {
        this.client = client;
        this.objectMapper = objectMapper;
        this.environment = environment;
    }

    public PostResponse createSchema(Resource resource) throws IOException {

        String contentAsString = resource.getContentAsString(Charset.defaultCharset());

        return client.post()
                .uri(uriBuilder -> uriBuilder.path("schemas").build())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(contentAsString)
                .retrieve()
                .bodyToMono(PostResponse.class)
                .block();

    }

    public PostResponse createTable(Resource resource) throws IOException {

        String contentAsString = resource.getContentAsString(Charset.defaultCharset());

        return client.post()
                .uri(uriBuilder -> uriBuilder.path("tables").build())
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(contentAsString)
                .retrieve()
                .bodyToMono(PostResponse.class)
                .block();

    }

    public PostResponse ingestFromFile(String tableName, BatchIngestConfiguration configuration, Resource resource) throws IOException {

        String configurationAsString = objectMapper.writeValueAsString(configuration);

        log.debug("batch configuration as string: {}", configurationAsString);

        MultipartBodyBuilder bodyBuilder = new MultipartBodyBuilder();
        bodyBuilder.part("file", resource);

        UriComponents uriComponents = UriComponentsBuilder.fromUriString(environment.getProperty("pinot.controller.url") + "/ingestFromFile")
                .queryParam("tableNameWithType", tableName)
                .queryParam("batchConfigMapStr", configurationAsString)
                .build();

        log.debug("uri: {}", uriComponents);

        return client.post()
                .uri(uriComponents.toUri())
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(bodyBuilder.build()))
                .retrieve()
                .bodyToMono(PostResponse.class)
                .block();

    }
}
