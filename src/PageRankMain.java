import algorithm.pagerank.PageRankDriver;
import graph.Graph;
import graph.partition.PageRankPartition;
import graph.GraphUtil;
import java.util.concurrent.TimeUnit;

public class PageRankMain
{
    /**
     * USER : Set the PageRank Configuration
     **/
    public static void main(String[] args)
            throws InterruptedException {
        final boolean isDirected = true;
        final boolean isWeighted = false;
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double percentage = Double.parseDouble(args[2]);

        double dampingFactor = 0.85;
        int iteration = 10;
        int expOfPartitionSize = 16;//1 << 12;      // 2 ^ n     For PartitionSize
        int asyncRangeSize = (int) ((1 << expOfPartitionSize) * percentage);

        Graph<PageRankPartition> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);

        long start = System.currentTimeMillis();
        System.out.println("[DEBUG] Graph Loading... ");
        GraphUtil.load(graph, inputFile);
        System.out.println("[DEBUG] Loading Time : " + (System.currentTimeMillis() - start));
        graph.generatePartition(asyncRangeSize, PageRankPartition.class);

        PageRankDriver driver = new PageRankDriver(graph, dampingFactor, iteration, numThreads);

        /**     PageRank Start      **/
        long[] elapsedTime = new long[20];

        System.out.println("[DEBUG] PageRank running .... ");
        GraphUtil.load(graph, inputFile);
        for (int i = 0; i < 20; i++) {
            driver.reset();

            if (i == 10) {
                System.out.println("START");
                TimeUnit.SECONDS.sleep(5);
            }

            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;

            if (i >= 10) {
                System.out.println("[DEBUG] elapsed time for iteration" + (i-10) + " : " + ((elapsedTime[i]) / (1000.0)));
            }

            // For Testing.. (perf)
            if (i == 10) {
                break;
            }
        }

        System.exit(1);
    }
}