import algorithm.sssp.SSSPDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.SSSPPartition;

import java.util.concurrent.BrokenBarrierException;

public class SSSPMain {
    public static void main(String[] args)
            throws BrokenBarrierException, InterruptedException {
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double delta = Double.parseDouble(args[2]);

        int expOfPartitionSize = 16; // 1 << 16;
        int asyncRangeSize = ((1 << 16) * 0); // 1 for async, 0 for sync

        long start = System.currentTimeMillis();

        Graph<SSSPPartition> graph = Graph.getInstance(expOfPartitionSize,true,true);
        System.out.println("[DEBUG] Graph Loading ... ");
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(asyncRangeSize, SSSPPartition.class);

        System.out.println("[DEBUG] Loading Time : " + (System.currentTimeMillis() - start) / 1000.0);

        SSSPDriver driver = new SSSPDriver(graph, numThreads, delta, 0);

        long[] elapsedTime = new long[20];

        System.out.println("[DEBUG] SSSP Start");
        for (int i = 0; i < 20; i++) {
            driver.reset();
            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;
            System.out.println("[DEBUG] elapsed time for iteration" + i + " : " + (elapsedTime[i] / (double) 1000));
            if (i == 0) {
                break;
            }
        }
        System.out.println("[DEBUG] SSSP END");
        System.out.print("[DEBUG] FileWrite ...");
        driver.print();
        System.exit(1);
    }
}