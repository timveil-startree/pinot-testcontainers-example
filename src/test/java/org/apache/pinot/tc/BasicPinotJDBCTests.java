package org.apache.pinot.tc;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.client.PinotDriver;
import org.apache.pinot.tc.api.PostResponse;
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

import java.sql.*;
import java.time.Duration;
import java.util.Properties;

@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BasicPinotJDBCTests {

    private static final Logger log = LoggerFactory.getLogger(BasicPinotJDBCTests.class);

    static Network pinotNetwork = Network.newNetwork();

    static ApachePinotCluster pinotCluster = new ApachePinotCluster("zookeeper:3.9", "apachepinot/pinot:latest-21-openjdk", false, pinotNetwork);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("pinot.controller.url", () -> String.format("http://localhost:%d", pinotCluster.getControllerPort()));
        registry.add("pinot.broker.url", () -> String.format("http://localhost:%d", pinotCluster.getBrokerPort()));
    }

    @Autowired
    private ControllerService controllerService;

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

        registerDriver();

        Properties properties = new Properties();
        properties.put("useMultistageEngine", "false");

        jdbcQuery(properties);

    }

    private void registerDriver() {
        try {
            DriverManager.registerDriver(new PinotDriver());
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void jdbcQuery(Properties properties) {

        String url = "jdbc:pinot://localhost:%s?brokers=localhost:%s".formatted(pinotCluster.getControllerPort(), pinotCluster.getBrokerPort());

        try (Connection conn = DriverManager.getConnection(url, properties);
             Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery("select avg(score) from transcript");


            while (rs.next()) {
                String averageScore = rs.getString(1);
                System.out.println(String.format("Average Score = %s", averageScore));
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
