package org.apache.pinot.tc;

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
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MinionTests {

    private static final Logger log = LoggerFactory.getLogger(MinionTests.class);
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("localstack/localstack:latest");
    private static final PropertyPlaceholderHelper PROPERTY_PLACEHOLDER_HELPER = new PropertyPlaceholderHelper("${", "}");
    private static final String BUCKET_NAME = "data";

    static Network pinotNetwork = Network.newNetwork();

    @Container
    static ApachePinotCluster pinotCluster = new ApachePinotCluster("arm64v8/zookeeper:3.6.3", "pinot:latest-11", true, pinotNetwork);

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(DOCKER_IMAGE_NAME)
            .withNetwork(pinotNetwork)
            .withNetworkAliases("localstack")
            .withEnv("HOSTNAME_EXTERNAL", "localstack")
            .withServices(S3);

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
        localstack.start();

        loadDataIntoS3();

        pinotCluster.start();
    }

    @AfterAll
    static void afterAll() {
        try {
            Thread.sleep(Duration.ofMinutes(20));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
//        pinotCluster.stop();
//        localstack.stop();
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
            Properties properties = new Properties();
            properties.setProperty("bucket", BUCKET_NAME);
            properties.setProperty("endpoint", "http://localstack:4566");
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
            PostResponse response = controllerService.scheduleTask("SegmentGenerationAndPushTask","transcript_OFFLINE");
            Assertions.assertNotNull(response);
            //Assertions.assertTrue(StringUtils.containsIgnoreCase(response.getStatus(), "successfully added"), "response was: %s".formatted(response));
            log.debug("schedule task response: {}", response);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            Assertions.fail(e);
        }
    }

    @Test
    @Order(4)
    void testSingleStageQuery() throws InterruptedException {

        Thread.sleep(Duration.ofSeconds(2));

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

    private static void loadDataIntoS3() throws IOException {
        try (S3Client s3 = S3Client
                .builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                        )
                )
                .region(Region.of(localstack.getRegion()))
                .build()) {


            s3.createBucket(b -> b.bucket(BUCKET_NAME));


            ListBucketsResponse listBucketsResponse = s3.listBuckets();
            List<Bucket> buckets = listBucketsResponse.buckets();

            for (Bucket bucket : buckets) {
                log.info("bucket: {}", bucket.toString());
            }

            final String fileName = "transcripts.csv";
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(fileName)
                    .build();

            ClassPathResource classPathResource = new ClassPathResource(fileName);

            s3.putObject(putObjectRequest, RequestBody.fromFile(classPathResource.getFile()));

            ListObjectsResponse listObjectsResponse = s3.listObjects(ListObjectsRequest.builder().bucket(BUCKET_NAME).build());
            for (S3Object content : listObjectsResponse.contents()) {
                log.info("s3 object: {}", content.toString());
            }
        }
    }

}
