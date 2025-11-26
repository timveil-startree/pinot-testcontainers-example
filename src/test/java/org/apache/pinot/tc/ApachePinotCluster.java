package org.apache.pinot.tc;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.stream.Stream;

public class ApachePinotCluster implements Startable {

    public static final String DEFAULT_PINOT_VERSION = "apachepinot/pinot:1.4.0-21-ms-openjdk";
    public static final String DEFAULT_ZOOKEEPER_VERSION = "zookeeper:3.9";
    public static final int LOCALSTACK_PORT = 4566;

    private static final DockerImageName LOCALSTACK_IMAGE_NAME = DockerImageName.parse("localstack/localstack:stable");

    private static final Logger log = LoggerFactory.getLogger(ApachePinotCluster.class);

    private static final int ZOOKEEPER_PORT = 2181;
    private static final String ZOOKEEPER_ALIAS = "zookeeper";

    private static final int CONTROLLER_PORT = 9000;
    private static final String CONTROLLER_ALIAS = "pinot-controller";
    private static final String CONTROLLER_COMMAND = "StartController -zkAddress %s:%s".formatted(ZOOKEEPER_ALIAS, ZOOKEEPER_PORT);

    private static final int BROKER_PORT = 8099;
    private static final String BROKER_ALIAS = "pinot-broker";
    private static final String BROKER_COMMAND = "StartBroker -zkAddress %s:%s".formatted(ZOOKEEPER_ALIAS, ZOOKEEPER_PORT);

    private static final int SERVER_PORT = 8098;
    private static final String SERVER_ALIAS = "pinot-server";
    private static final String SERVER_COMMAND = "StartServer -zkAddress %s:%s".formatted(ZOOKEEPER_ALIAS, ZOOKEEPER_PORT);

    private static final String MINION_ALIAS = "pinot-minion";
    private static final String MINION_COMMAND = "StartMinion -zkAddress %s:%s".formatted(ZOOKEEPER_ALIAS, ZOOKEEPER_PORT);

    private static final String JAVA_OPTS = "JAVA_OPTS";

    private final GenericContainer<?> zookeeper;

    private final GenericContainer<?> pinotController;

    private final GenericContainer<?> pinotBroker;

    private final GenericContainer<?> pinotServer;

    private GenericContainer<?> pinotMinion;

    private LocalStackContainer localStack;

    private final boolean enableMinion;

    private final boolean enableLocalstack;

    public ApachePinotCluster(Boolean enableMinion, Boolean enableLocalstack) {
        this(DEFAULT_ZOOKEEPER_VERSION, DEFAULT_PINOT_VERSION, Network.newNetwork(), enableMinion, enableLocalstack);
    }

    public ApachePinotCluster(Network network, Boolean enableMinion, Boolean enableLocalstack) {
        this(DEFAULT_ZOOKEEPER_VERSION, DEFAULT_PINOT_VERSION, network, enableMinion, enableLocalstack);
    }

    public ApachePinotCluster(String zookeeperVersion, String pinotVersion, Network network, Boolean enableMinion, Boolean enableLocalstack) {
        this.enableMinion = enableMinion;
        this.enableLocalstack = enableLocalstack;

        this.zookeeper =
                new GenericContainer<>(zookeeperVersion)
                        .withNetwork(network)
                        .withNetworkAliases(ZOOKEEPER_ALIAS)
                        .withExposedPorts(ZOOKEEPER_PORT)
                        .withEnv("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_PORT))
                        .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                        .withStartupTimeout(Duration.ofMinutes(1))
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(ZOOKEEPER_ALIAS)));

        this.pinotController =
                new GenericContainer<>(pinotVersion)
                        .withNetwork(network)
                        .withNetworkAliases(CONTROLLER_ALIAS)
                        .dependsOn(zookeeper)
                        .withExposedPorts(CONTROLLER_PORT)
                        .withEnv(JAVA_OPTS, getJavaOpts("1G", "4G"))
                        .withEnv("LOG4J_CONSOLE_LEVEL", "warn")
                        .withCommand(CONTROLLER_COMMAND)
                        .waitingFor(getWaitStrategy("CONTROLLER"))
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(CONTROLLER_ALIAS)));

        this.pinotBroker =
                new GenericContainer<>(pinotVersion)
                        .withNetwork(network)
                        .withNetworkAliases(BROKER_ALIAS)
                        .dependsOn(pinotController)
                        .withExposedPorts(BROKER_PORT)
                        .withEnv(JAVA_OPTS, getJavaOpts("4G", "4G"))
                        .withEnv("LOG4J_CONSOLE_LEVEL", "warn")
                        .withCommand(BROKER_COMMAND)
                        .waitingFor(getWaitStrategy("BROKER"))
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(BROKER_ALIAS)));

        this.pinotServer =
                new GenericContainer<>(pinotVersion)
                        .withNetwork(network)
                        .withNetworkAliases(SERVER_ALIAS)
                        .dependsOn(pinotBroker)
                        .withExposedPorts(SERVER_PORT)
                        .withEnv(JAVA_OPTS, getJavaOpts("4G", "8G"))
                        .withEnv("LOG4J_CONSOLE_LEVEL", "warn")
                        .withCommand(SERVER_COMMAND)
                        .waitingFor(getWaitStrategy("SERVER"))
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(SERVER_ALIAS)));

        if (enableMinion) {
            this.pinotMinion =
                    new GenericContainer<>(pinotVersion)
                            .withNetwork(network)
                            .withNetworkAliases(MINION_ALIAS)
                            .dependsOn(pinotBroker)
                            .withEnv(JAVA_OPTS, getJavaOpts("4G", "8G"))
                            .withEnv("LOG4J_CONSOLE_LEVEL", "warn")
                            .withCommand(MINION_COMMAND)
                            .waitingFor(getWaitStrategy("MINION"))
                            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(MINION_ALIAS)));
        }

        if (enableLocalstack) {
            this.localStack = new LocalStackContainer(LOCALSTACK_IMAGE_NAME)
                    .withNetwork(network)
                    .withNetworkAliases("localstack")
                    .withEnv("LOCALSTACK_HOST", "localstack")
                    .withExposedPorts(LOCALSTACK_PORT)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("localstack")))
                    .withServices("s3");

        }
    }

    @Override
    public void start() {
        log.info("starting all containers");
        allContainers().sequential().forEach(GenericContainer::start);
    }

    @Override
    public void stop() {
        log.info("stopping all containers");
        allContainers().parallel().forEach(GenericContainer::stop);
    }

    @Override
    public void close() {
        log.info("closing all containers");
        allContainers().parallel().forEach(GenericContainer::close);
    }

    public String getServerLogs() {
        return pinotServer.getLogs();
    }

    public String getBrokerLogs() {
        return pinotBroker.getLogs();
    }

    public String getControllerLogs() {
        return pinotBroker.getLogs();
    }

    public String getMinionLogs() {
        if (pinotMinion != null) {
            return pinotMinion.getLogs();
        }

        return null;
    }

    public String getLocalStackLogs() {
        if (localStack != null) {
            return localStack.getLogs();
        }

        return null;
    }

    public LocalStackContainer getLocalStack() {
        return localStack;
    }

    @NotNull
    private static String getJavaOpts(String xms, String xmx) {
        return "-Dplugins.dir=/opt/pinot/plugins -Xms%s -Xmx%s -XX:+UseG1GC -XX:MaxGCPauseMillis=200".formatted(xms, xmx);
    }

    private static LogMessageWaitStrategy getWaitStrategy(String service) {
        return Wait.forLogMessage("^(?:.*?)Started Pinot \\[%s\\] instance(?:.*?)$".formatted(service), 1);
    }

    private Stream<GenericContainer<?>> allContainers() {
        Stream<GenericContainer<?>> stream = Stream.of(this.zookeeper, this.pinotController, this.pinotBroker, this.pinotServer);

        if (enableMinion) {
            stream = Stream.concat(stream, Stream.of(this.pinotMinion));
        }

        if (enableLocalstack) {
            stream = Stream.concat(stream, Stream.of(this.localStack));
        }

        return stream;
    }

    public int getControllerPort() {
        return pinotController.getMappedPort(CONTROLLER_PORT);
    }

    public int getBrokerPort() {
        return pinotBroker.getMappedPort(BROKER_PORT);
    }

    public int getLocalstackPort() {
        return localStack.getMappedPort(LOCALSTACK_PORT);
    }


}
