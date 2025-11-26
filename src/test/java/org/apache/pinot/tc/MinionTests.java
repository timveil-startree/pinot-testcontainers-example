package org.apache.pinot.tc;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.tc.api.PostResponse;
import org.apache.pinot.tc.api.QueryResponse;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.PropertyPlaceholderHelper;
import org.testcontainers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Properties;


@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MinionTests {

    private static final Logger log = LoggerFactory.getLogger(MinionTests.class);

    private static final PropertyPlaceholderHelper PROPERTY_PLACEHOLDER_HELPER = new PropertyPlaceholderHelper("${", "}");

    private static final String BUCKET_NAME = "data";

    static ApachePinotCluster pinotCluster = new ApachePinotCluster(true, true);

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

    @Value("classpath:transcript-table-offline-minion.json")
    private Resource transcriptTableDefinition;

    @BeforeAll
    static void beforeAll() throws IOException {
        pinotCluster.start();

        loadDataIntoS3();
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

    @AfterAll
    static void afterAll() {
        pinotCluster.stop();
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

            LocalStackContainer localstack = pinotCluster.getLocalStack();

            Properties properties = new Properties();
            properties.setProperty("bucket", BUCKET_NAME);
            properties.setProperty("endpoint", String.format("http://localstack:%d", ApachePinotCluster.LOCALSTACK_PORT));
            properties.setProperty("region", localstack.getRegion());
            properties.setProperty("secretKey", localstack.getSecretKey());
            properties.setProperty("accessKey", localstack.getAccessKey());

            log.debug(properties.toString());

            String tableConfig = PROPERTY_PLACEHOLDER_HELPER.replacePlaceholders(transcriptTableDefinition.getContentAsString(Charset.defaultCharset()), properties);

            PostResponse response = controllerService.createTable(tableConfig);
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
    void testScheduleTaskData() {
        try {
            String response = controllerService.scheduleTask("SegmentGenerationAndPushTask", "transcript_OFFLINE");
            Assertions.assertNotNull(response);
            Assertions.assertTrue(StringUtils.containsIgnoreCase(response, "Task_SegmentGenerationAndPushTask_"), "task id not valid: %s".formatted(response));
            log.debug("schedule task id: {}", response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e);
        }
    }

    @Test
    @Order(4)
    void testSingleStageQuery() {

        try {
            Thread.sleep(Duration.ofMinutes(2));

            QueryResponse response = brokerService.executeQuery("select avg(score) from transcript");
            Assertions.assertNotNull(response);
            Assertions.assertEquals(1, response.getNumRowsResultSet());
            log.debug("query response: {}", response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e);
        }
    }

    private static void loadDataIntoS3() throws IOException {

        LocalStackContainer localstack = pinotCluster.getLocalStack();

        S3ClientBuilder clientBuilder = S3Client
                .builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                        )
                )
                .region(Region.of(localstack.getRegion()));

        try (S3Client s3 = clientBuilder.build()) {
            s3.createBucket(b -> b.bucket(BUCKET_NAME));

            final String fileName = "transcripts.csv";
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(fileName)
                    .build();

            ClassPathResource classPathResource = new ClassPathResource(fileName);
            s3.putObject(putObjectRequest, RequestBody.fromFile(classPathResource.getFile()));
        }
    }

}
