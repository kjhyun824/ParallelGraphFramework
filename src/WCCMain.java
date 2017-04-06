import algorithm.wcc.WCCDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.IntegerPartition;

public class WCCMain {
    public static void main(String[] args) {
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);

        int expOfPartitionSize = 16; // 1 << 16;
        int numValuesPerNode = 1;
        int asyncRangeSize = ((1 << 16) * 0);

        long start = System.currentTimeMillis();
        Graph<IntegerPartition> graph = Graph.getInstance(expOfPartitionSize,false,false);
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(numValuesPerNode, asyncRangeSize, IntegerPartition.class);
        long loadingTime = System.currentTimeMillis() - start;

        System.out.println("[DEBUG] Graph Loading : " + ((double) loadingTime / 1000.0));

        WCCDriver driver = new WCCDriver(graph, numThreads);

        long[] elapsedTime = new long[20];

        for (int i = 0; i < 20; i++) {
            driver.reset();
            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;
            if (i >= 10) {
                System.out.println("[DEBUG] elapsed time for iteration" + (i-10) + " : " + (((double) elapsedTime[i]) / (1000.0)));
            }
        }
        System.exit(1);
    }
}