package org.apache.pinot.tc;

import org.testcontainers.containers.Network;

import java.time.Duration;

public class ApachePinotClusterTest {

    public static void main(String[] args) {
        try (ApachePinotCluster cluster = new ApachePinotCluster("arm64v8/zookeeper:3.9", "apachepinot/pinot:latest-21-openjdk", false, Network.newNetwork())) {
            cluster.start();

            Thread.sleep(Duration.ofMinutes(5));

            cluster.stop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
