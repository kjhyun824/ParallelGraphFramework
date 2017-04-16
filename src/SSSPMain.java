import algorithm.sssp.SSSPDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.SSSPPartition;

public class SSSPMain {
    public static void main(String[] args) {
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double delta = Double.parseDouble(args[2]);

        int expOfPartitionSize = 16; // 1 << 16;
        int numValuesPerNode = 1;
        int asyncRangeSize = ((1 << 16) * 0); // 1 for async, 0 for sync

        long start = System.currentTimeMillis();

        Graph<SSSPPartition> graph = Graph.getInstance(expOfPartitionSize,true,true);
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(numValuesPerNode, asyncRangeSize, SSSPPartition.class);
        //GraphUtil.finalizeLoading(graph);

        long loadingTime = System.currentTimeMillis() - start;
        System.out.println("[DEBUG] Graph Loading : " + ((double) loadingTime / 1000.0));

        SSSPDriver driver = new SSSPDriver(graph, numThreads, delta);

        long[] elapsedTime = new long[20];

        for (int i = 0; i < 20; i++) {
            driver.reset();
            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;
            System.out.println("[DEBUG] elapsed time for iteration" + i + " : " + (elapsedTime[i] / (double) 1000));
        }

        System.exit(1);
    }
}