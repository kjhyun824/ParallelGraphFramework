import algorithm.pagerank.original.PageRankDriver;
import graph.Graph;
import graph.partition.PageRankPartition;
import graph.GraphUtil;

import java.util.concurrent.BrokenBarrierException;

public class PageRankMain
{
    /**
     * USER : Set the PageRank Configuration
     **/
    public static void main(String[] args)
            throws InterruptedException, BrokenBarrierException {
        final boolean isDirected = true;
        final boolean isWeighted = false;

        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double asyncPercentage = Double.parseDouble(args[2]);

        double dampingFactor = 0.85;
        int iteration = 10;
        int expOfPartitionSize = 16;//1 << 12;      // 2 ^ n     For PartitionSize
        int asyncRangeSize = (int) ((1 << expOfPartitionSize) * asyncPercentage);

        Graph<PageRankPartition> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);

        long start = System.currentTimeMillis();
        System.err.println("Graph Loading... ");
        GraphUtil.load(graph, inputFile);
        System.err.println("Loading Time : " + (System.currentTimeMillis() - start) / 1000.0);
        graph.generatePartition(asyncRangeSize, PageRankPartition.class);

        PageRankDriver driver = new PageRankDriver(graph, dampingFactor, iteration, numThreads);

        /**     PageRank Start      **/
        double timeSum = 0;

        System.err.println("PageRank Running ... ");
        final int numRun = 15;
        long[] elapsedTime = new long[numRun];
        for (int i = 0; i < numRun; i++) {
            driver.reset();
            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;
            System.err.println("elapsed time for iteration" + i + " : " + ((elapsedTime[i]) / (1000.0)));

            if (i >= 5) {
                timeSum += (elapsedTime[i] / 1000.0);
            }
        }
        System.err.println("PageRank Complete ");
        String averageTime = String.format("%.3f", (timeSum / 10));
        System.out.println(driver._printPageRankSum() + "/" + averageTime);

        System.exit(1);
    }
}