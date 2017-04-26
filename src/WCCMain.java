import algorithm.wcc.WCCDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.WCCPartition;

import java.util.concurrent.BrokenBarrierException;

public class WCCMain {
    public static void main(String[] args)
            throws BrokenBarrierException, InterruptedException {

        final boolean isDirected = false;
        final boolean isWeighted = false;
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double asyncPercent = Double.parseDouble(args[2]);
        int expOfPartitionSize = Integer.parseInt(args[3]);
        int seed = Integer.parseInt(args[4]);

        System.out.println("[DEBUG] Input File : " + inputFile);
        System.out.println("[DEBUG] NUM_THREAD : " + numThreads);
        System.out.println("[DEBUG] EXP_OF_PARTITION_SIZE : " + expOfPartitionSize);

        int asyncRangeSize = (int) ((1 << expOfPartitionSize) * asyncPercent);
        System.out.println("[DEBUG] ASYNC_SIZE : " + asyncRangeSize);
        if (asyncPercent == 1) {
            System.out.println("[DEBUG] ASYNC");
        } else {
            System.out.println("[DEBUG] ATOMIC");
        }
        Graph<WCCPartition> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);

        long start = System.currentTimeMillis();
        System.out.println("[DEBUG] Graph Loading ... ");
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(asyncRangeSize, WCCPartition.class);
        long loadingTime = System.currentTimeMillis() - start;

        System.out.println("[DEBUG] Loading Time : " + ((double) loadingTime / 1000.0));
        System.out.println("[DEBUG] Num Partitions : " + graph.getNumPartitions());

        WCCDriver driver = new WCCDriver(graph, numThreads, seed);

        final int numRun = 20;
        long[] elapsedTime = new long[numRun];
        double timeSum = 0;

        System.out.println("[DEBUG] WCC Running ... ");
        for (int i = 0; i < numRun; i++) {
            driver.reset();

            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;

            System.out.println("[DEBUG] elapsed time for iteration" + i + " : " + ((elapsedTime[i]) / (1000.0)));
            System.out.println("[DEBUG] Largest WCC : " + driver.getLargestWCC());

/*
            if (i == 9) {
                System.out.println("[DEBUG] Garbage Collecting");
                System.gc();
                System.gc();
                System.gc();
            }
*/

            if (i >= 10) {
                timeSum += (elapsedTime[i] / 1000.0);
            }
        }

        System.out.println("");

        String averageTime = String.format("%.3f", (timeSum / 10));
        System.out.println("[DEBUG] Average Elapsed time : " + averageTime);

        System.exit(1);
    }
}