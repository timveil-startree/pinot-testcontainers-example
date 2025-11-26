package org.apache.pinot.tc;

import java.time.Duration;

public class MinionClusterTest {

    public static void main(String[] args) {
        try (ApachePinotCluster cluster = new ApachePinotCluster(true, true)) {
            cluster.start();

            Thread.sleep(Duration.ofMinutes(5));

            cluster.stop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
