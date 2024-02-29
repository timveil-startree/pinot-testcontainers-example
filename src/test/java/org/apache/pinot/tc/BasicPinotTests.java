package org.apache.pinot.tc;

import org.apache.pinot.tc.api.PostResponse;
import org.apache.pinot.tc.api.QueryResponse;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

import java.time.Duration;

@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BasicPinotTests {

    private static final Logger log = LoggerFactory.getLogger(BasicPinotTests.class);

    static Network pinotNetwork = Network.newNetwork();

    static ApachePinotCluster pinotCluster = new ApachePinotCluster("arm64v8/zookeeper:3.7.2", "apachepinot/pinot:latest-21-openjdk", false, pinotNetwork);

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

    @BeforeEach
    void setUp() {
        try {
            log.error("taking a quick nap before starting test...");
            Thread.sleep(Duration.ofSeconds(10L));
        } catch (InterruptedException e) {
            log.error("error while sleeping: {}", e.getMessage(), e);
        }
    }

    @Test
    @Order(1)
    void testCreateSchema() {
        try {
            PostResponse response = controllerService.createSchema(transcriptSchemaDefinition);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(StringUtils.containsIgnoreCase(response.getStatus(), "successfully added"), "response was: %s".formatted(response));
            log.debug("create schema response: {}", response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e);
        }
    }

    @Test
    @Order(2)
    void testCreateTable() {
        try {
            PostResponse response = controllerService.createTable(transcriptTableDefinition);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(StringUtils.containsIgnoreCase(response.getStatus(), "successfully added"), "response was: %s".formatted(response));
            log.debug("create table response: {}", response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e);
        }
    }

    @Test
    @Order(3)
    void testIngestData() {
        try {
            PostResponse response = controllerService.ingestFromFile("transcript_OFFLINE", new BatchIngestConfiguration("csv", ","), transcriptData);
            Assertions.assertNotNull(response);
            Assertions.assertTrue(StringUtils.containsIgnoreCase(response.getStatus(), "successfully ingested file into table"), "response was: %s".formatted(response));
            log.debug("ingest from file response: {}", response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e);
        }
    }

    @Test
    @Order(4)
    void testSingleStageQuery() {
        try {
            QueryResponse response = brokerService.executeQuery("select avg(score) from transcript");
            Assertions.assertNotNull(response);
            Assertions.assertEquals(1, response.getNumRowsResultSet());
            log.debug("query response: {}", response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e);
        }
    }

}
