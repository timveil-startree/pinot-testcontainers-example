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

import java.time.Duration;
import java.util.stream.Stream;

public class ApachePinotCluster implements Startable {

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

    private final boolean enableMinion;

    public ApachePinotCluster(String zookeeperVersion, String pinotVersion, Boolean enableMinion) {
        this.enableMinion = enableMinion;

        Network pinotNetwork = Network.newNetwork();

        this.zookeeper =
                new GenericContainer<>("arm64v8/zookeeper:%s".formatted(zookeeperVersion))
                        .withNetwork(pinotNetwork)
                        .withNetworkAliases(ZOOKEEPER_ALIAS)
                        .withExposedPorts(ZOOKEEPER_PORT)
                        .withEnv("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_PORT))
                        .withEnv("ZOOKEEPER_TICK_TIME", "2000")
                        .withStartupTimeout(Duration.ofMinutes(1))
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(ZOOKEEPER_ALIAS)));

        this.pinotController =
                new GenericContainer<>("apachepinot/pinot:%s".formatted(pinotVersion))
                        .withNetwork(pinotNetwork)
                        .withNetworkAliases(CONTROLLER_ALIAS)
                        .dependsOn(zookeeper)
                        .withExposedPorts(CONTROLLER_PORT)
                        .withEnv(JAVA_OPTS, getJavaOpts("1G", "4G"))
                        .withEnv("LOG4J_CONSOLE_LEVEL", "warn")
                        .withCommand(CONTROLLER_COMMAND)
                        .waitingFor(getWaitStrategy("CONTROLLER"))
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(CONTROLLER_ALIAS)));

        this.pinotBroker =
                new GenericContainer<>("apachepinot/pinot:%s".formatted(pinotVersion))
                        .withNetwork(pinotNetwork)
                        .withNetworkAliases(BROKER_ALIAS)
                        .dependsOn(pinotController)
                        .withExposedPorts(BROKER_PORT)
                        .withEnv(JAVA_OPTS, getJavaOpts("4G", "4G"))
                        .withEnv("LOG4J_CONSOLE_LEVEL", "warn")
                        .withCommand(BROKER_COMMAND)
                        .waitingFor(getWaitStrategy("BROKER"))
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(BROKER_ALIAS)));

        this.pinotServer =
                new GenericContainer<>("apachepinot/pinot:%s".formatted(pinotVersion))
                        .withNetwork(pinotNetwork)
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
                    new GenericContainer<>("apachepinot/pinot:%s".formatted(pinotVersion))
                            .withNetwork(pinotNetwork)
                            .withNetworkAliases(MINION_ALIAS)
                            .dependsOn(pinotBroker)
                            .withEnv(JAVA_OPTS, getJavaOpts("4G", "8G"))
                            .withEnv("LOG4J_CONSOLE_LEVEL", "warn")
                            .withCommand(MINION_COMMAND)
                            .waitingFor(getWaitStrategy("MINION"))
                            .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(MINION_ALIAS)));
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

        return stream;
    }

    public int getControllerPort() {
        return pinotController.getMappedPort(CONTROLLER_PORT);
    }

    public int getBrokerPort() {
        return pinotBroker.getMappedPort(BROKER_PORT);
    }


}
