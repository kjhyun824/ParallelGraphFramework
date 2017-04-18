import algorithm.sssp.SSSPDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.SSSPPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BrokenBarrierException;

public class SSSPMain
{
    public static void main(String[] args)
            throws BrokenBarrierException, InterruptedException {
        final boolean isDirected = true;
        final boolean isWeighted = true;
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double delta = Double.parseDouble(args[2]);

        int expOfPartitionSize = 16; // 1 << 16;
        int asyncRangeSize = ((1 << 16) * 0); // 1 for async, 0 for sync

        long start = System.currentTimeMillis();

        Graph<SSSPPartition> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);
        System.out.println("[DEBUG] Graph Loading ... ");
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(asyncRangeSize, SSSPPartition.class);

        System.out.println("[DEBUG] Loading Time : " + (System.currentTimeMillis() - start) / 1000.0);

        SSSPDriver driver = new SSSPDriver(graph, numThreads, delta, 0);

        long[] elapsedTime = new long[20];
        double timeSum = 0;

        System.out.println("[DEBUG] SSSP Running ... ");
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
        System.out.println("[DEBUG] SSSP Complete : ");
        System.out.println("[DEBUG] File Write ...");

        try (FileWriter fw = new FileWriter("SSSP.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
            String averageTime = String.format("%.3f", (timeSum / 10));
            out.println(averageTime);
        }
        catch (IOException e) {

        }

        System.exit(1);
    }
}