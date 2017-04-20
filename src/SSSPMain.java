import algorithm.sssp.SSSPDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.SSSPPartition;

import java.util.concurrent.BrokenBarrierException;

public class SSSPMain
{
    public static void main(String[] args)
            throws BrokenBarrierException, InterruptedException {
        final boolean isDirected = true;
        final boolean isWeighted = true;
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        int delta = Integer.parseInt(args[2]);
        int sync = Integer.parseInt(args[3]);

        int expOfPartitionSize = 16; // 1 << 16;
        int asyncRangeSize = ((1 << 16) * sync); // 1 for async, 0 for sync

        long start = System.currentTimeMillis();

        Graph<SSSPPartition> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);
        System.err.println("Graph Loading ... ");
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(asyncRangeSize, SSSPPartition.class);

        System.err.println("Loading Time : " + (System.currentTimeMillis() - start) / 1000.0);

        SSSPDriver driver = new SSSPDriver(graph, numThreads, delta, 0);

        final int numRun = 20;
        long[] elapsedTime = new long[numRun];
        double timeSum = 0;

        System.err.println("SSSP Running ... ");

        for (int i = 0; i < numRun; i++) {
            driver.reset();
            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;
            System.err.println("elapsed time for iteration" + i + " : " + ((elapsedTime[i]) / (1000.0)));

            if (i >= 10) {
                timeSum += (elapsedTime[i] / 1000.0);
            }
        }
        System.err.println("SSSP Complete : ");
        System.err.println("File Write ...");

        String averageTime = String.format("%.3f", (timeSum / 10));
        System.out.println(averageTime);

        System.exit(1);
    }
}