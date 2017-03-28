import algorithm.pagerank.PageRankDriver;
import graph.DirectedGraph;
import graph.GraphPartition;
import graph.GraphUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class PageRankMain {
    /**
     * USER : Set the PageRank Configuration
     **/
    public static void main(String[] args) {
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);

        double dampingFactor = 0.85;
        int iteration = 10;
        int expOfPartitionSize = 16;//1 << 12;      // 2 ^ n     For PartitionSize
        int numValuesPerNode = 2;
        double percentage = 1;
        int asyncRangeSize = (int) ((1 << 16) * percentage);

        DirectedGraph graph = DirectedGraph.getInstance();
        GraphUtil.load(graph, inputFile);
        GraphPartition graphPartition = graph.createPartitionInstance(expOfPartitionSize);
        graphPartition.generate(numValuesPerNode, asyncRangeSize);
        GraphUtil.finalizeLoading(graph);

        PageRankDriver driver = new PageRankDriver(graph, dampingFactor, iteration, numThreads);

        /**     PageRank Start      **/
        long[] elapsedTime = new long[20];

        for (int i = 0; i < 20; i++) {
            long start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;
            driver.reset();
        }

        System.out.println("Async 100%, Atomic 0%\n");
        double timeSum = 0;
        for (int i = 0; i < 10; i++) {
            System.out.print(elapsedTime[i + 10] / (double) 1000 + " ");
            timeSum += elapsedTime[i + 10] / (double) 1000;
        }
        System.out.print("AVG : " + timeSum / 10);

        try (FileWriter fw = new FileWriter(String.valueOf(percentage * 100) + "out.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
            out.println("Async 100%, Atomic 0%");
            out.println("AVG : " + timeSum / 10);
        }
        catch (IOException e) {

        }

        System.exit(1);

    }

}