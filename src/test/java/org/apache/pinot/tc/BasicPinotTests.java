package org.apache.pinot.tc;

import org.apache.pinot.tc.api.PostResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

@SpringBootTest
class BasicPinotTests {

    private static final Logger log = LoggerFactory.getLogger(BasicPinotTests.class);

    @Container
    static ApachePinotCluster pinotCluster = new ApachePinotCluster("3.6.3", "release-1.0.0-21-openjdk", false);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("pinot.controller.url", () -> String.format("http://localhost:%d", pinotCluster.getControllerPort()));
        registry.add("pinot.broker.url", () -> String.format("http://localhost:%d", pinotCluster.getBrokerPort()));
    }

    @Autowired
    private ControllerService controllerService;

    @Autowired
    private BrokerService brokerService;

    @Value("classpath:transcript-schema.json")
    private Resource transcriptSchemaDefinition;

    @Value("classpath:transcript-table-offline.json")
    private Resource transcriptTableDefinition;

    @Value("classpath:transcripts.csv")
    private Resource transcriptData;

    @BeforeAll
    static void beforeAll() {
        pinotCluster.start();
    }

    @AfterAll
    static void afterAll() {
        pinotCluster.stop();
    }

    @Test
    void testCreateSchema() {
        try {
            PostResponse response = controllerService.createSchema(transcriptSchemaDefinition);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(StringUtils.containsIgnoreCase(response.getStatus(), "successfully added"), "response was: %s".formatted(response));
            log.debug("create schema response: {}", response);
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    void testCreateTable() {
        try {
            PostResponse response = controllerService.createTable(transcriptTableDefinition);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(StringUtils.containsIgnoreCase(response.getStatus(), "successfully added"), "response was: %s".formatted(response));
            log.debug("create table response: {}", response);
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    void ingestData() {
        try {
            PostResponse response = controllerService.ingestFromFile("transcript_OFFLINE", new BatchIngestConfiguration("csv", ","), transcriptData);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(StringUtils.containsIgnoreCase(response.getStatus(), "successfully ingested file into table"), "response was: %s".formatted(response));
            log.debug("ingest from file response: {}", response);
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

}
