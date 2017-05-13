import algorithm.pagerank.personalized.PersonalPageRankDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.sharedData.PersonalPageRankSharedData;

import java.util.concurrent.BrokenBarrierException;

public class PersonalizedPageRankMain
{
    /**
     * USER : Set the PageRank Configuration
     **/
    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
        final boolean isDirected = true;
        final boolean isWeighted = false;

        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        int asyncThreshold = Integer.parseInt(args[2]);
        int expOfPartitionSize = Integer.parseInt(args[3]);//1 << 12;      // 2 ^ n     For PartitionSize
        String seedFile = args[4];
        int numSeeds = Integer.parseInt(args[5]);

        double dampingFactor = 0.85;
        int iteration = 10;

        Graph<PersonalPageRankSharedData> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);

        long start = System.currentTimeMillis();
        System.err.println("Graph Loading... ");
        GraphUtil.load(graph, inputFile);
        graph.loadFinalize(asyncThreshold, PersonalPageRankSharedData.class);
        System.err.println("Loading Time : " + (System.currentTimeMillis() - start) / 1000.0);

        PersonalPageRankDriver driver = new PersonalPageRankDriver(graph, dampingFactor, numThreads, iteration, seedFile, numSeeds);

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
        System.err.println("Personalized_PageRank Complete ");
//        System.err.println("numActiveNodes : " + driver.getNumActiveNodes());
        String averageTime = String.format("%.3f", (timeSum / 10));
        System.out.println(driver._printPageRankSum() + "/" + averageTime);

        System.exit(1);
    }
}
