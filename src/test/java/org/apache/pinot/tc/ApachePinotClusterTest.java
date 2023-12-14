package org.apache.pinot.tc;

import java.time.Duration;

public class ApachePinotClusterTest {

    public static void main(String[] args) {
        try (ApachePinotCluster cluster = new ApachePinotCluster("3.6.3", "release-1.0.0-21-openjdk", false)) {
            cluster.start();

            Thread.sleep(Duration.ofMinutes(5));

            cluster.stop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
