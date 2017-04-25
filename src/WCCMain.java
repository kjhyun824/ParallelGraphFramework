import algorithm.wcc.WCCDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.WCCPartition;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

public class WCCMain
{
    public static void main(String[] args) throws BrokenBarrierException, InterruptedException, IOException {

        final boolean isDirected = false;
        final boolean isWeighted = false;
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double asyncPercent = Double.parseDouble(args[2]);
        int expOfPartitionSize = Integer.parseInt(args[3]);

        System.err.println("NUM_THREAD : " + numThreads);
        System.err.println("EXP_OF_PARTITION_SIZE : " + expOfPartitionSize);

        int asyncRangeSize = (int) ((1 << expOfPartitionSize) * asyncPercent);
        System.err.println("ASYNC_SIZE : " + asyncRangeSize);

        Graph<WCCPartition> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);

        long start = System.currentTimeMillis();
        System.err.println("Graph Loading ... ");
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(asyncRangeSize, WCCPartition.class);
        long loadingTime = System.currentTimeMillis() - start;

        System.err.println("Loading Time : " + ((double) loadingTime / 1000.0));

        WCCDriver driver = new WCCDriver(graph, numThreads);

        final int numRun = 20;
        long[] elapsedTime = new long[numRun];
        double timeSum = 0;

        System.err.println("WCC Running ... ");
        for (int i = 0; i < numRun; i++) {
            driver.reset();

            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;

            System.err.println("elapsed time for iteration" + i + " : " + ((elapsedTime[i]) / (1000.0)));
            System.err.println("Nodes in Largest WCC : " + driver.getLargestWCC());

            if (i >= 10) {
                timeSum += (elapsedTime[i] / 1000.0);
            }
        }
        System.err.println("");
        if (asyncPercent == 1) {
            System.err.println("ASYNC");
        }
        else {
            System.err.println("ATOMIC");
        }
        System.err.println("");
        System.err.println("WCC Complete : " + driver.getLargestWCC());

        String averageTime = String.format("%.3f", (timeSum / 10));
        System.out.println(driver.getLargestWCC() + "/" + averageTime);

        System.exit(1);
    }
}