import algorithm.pagerank.PageRankDriver;
import graph.Graph;
import graph.partition.PageRankPartition;
import graph.GraphUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
        System.out.println("[DEBUG] Graph Loading... ");
        GraphUtil.load(graph, inputFile);
        System.out.println("[DEBUG] Loading Time : " + (System.currentTimeMillis() - start) / 1000.0);
        graph.generatePartition(asyncRangeSize, PageRankPartition.class);

        PageRankDriver driver = new PageRankDriver(graph, dampingFactor, iteration, numThreads);

        /**     PageRank Start      **/
        long[] elapsedTime = new long[20];
        double timeSum = 0;

        System.out.println("[DEBUG] PageRank Running ... ");
        for (int i = 0; i < 20; i++) {
            driver.reset();
            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;

            if (i >= 10) {
                timeSum += (elapsedTime[i] / 1000.0);
//                System.out.println("[DEBUG] Average : " + (elapsedTime[i] / 1000.0) + "/");
//                System.out.println("[DEBUG] elapsed time for iteration" + (i-10) + " : " + ((elapsedTime[i]) / (1000.0)));
            }
        }
        System.out.println("[DEBUG] PageRank Complete : ");
        System.out.println("[DEBUG] File Write ...");

        try (FileWriter fw = new FileWriter("PageRank.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
            String averageTime = String.format("%.3f", (timeSum / 10));
            out.println(averageTime);
        }
        catch (IOException e) {

        }

        System.exit(1);
    }
}