package org.apache.pinot.tc;

import java.time.Duration;

public class ApachePinotClusterTest {

    public static void main(String[] args) {
        try (ApachePinotCluster cluster = new ApachePinotCluster(false, false)) {
            cluster.start();

            Thread.sleep(Duration.ofMinutes(5));

            cluster.stop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
