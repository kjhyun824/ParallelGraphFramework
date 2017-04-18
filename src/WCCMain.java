import algorithm.wcc.WCCDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.WCCPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BrokenBarrierException;

public class WCCMain
{
    public static void main(String[] args)
            throws BrokenBarrierException, InterruptedException {

        final boolean isDirected = false;
        final boolean isWeighted = false;
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double asyncPercent = Double.parseDouble(args[2]);

        int expOfPartitionSize = 16; // 1 << 16;
        int asyncRangeSize = (int) ((1 << 16) * asyncPercent);

        long start = System.currentTimeMillis();
        Graph<WCCPartition> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);
        System.out.println("[DEBUG] Graph Loading ... ");
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(asyncRangeSize, WCCPartition.class);
        long loadingTime = System.currentTimeMillis() - start;

        System.out.println("[DEBUG] Loading Time : " + ((double) loadingTime / 1000.0));

        WCCDriver driver = new WCCDriver(graph, numThreads);

        long[] elapsedTime = new long[20];
        double timeSum = 0;

        System.out.println("[DEBUG] WCC Running ... ");
        for (int i = 0; i < 20; i++) {
            driver.reset();

            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;

//            System.out.println(driver.getLargestWCC());
            if (i >= 10) {
                timeSum += (elapsedTime[i] / 1000.0);
//                System.out.println("[DEBUG] Average : " + (elapsedTime[i] / 1000.0) + "/");
//                System.out.println("[DEBUG] elapsed time for iteration" + (i-10) + " : " + ((elapsedTime[i]) / (1000.0)));
            }
        }
        System.out.println("[DEBUG] WCC Complete : " + driver.getLargestWCC());

        try (FileWriter fw = new FileWriter("WCC.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
            String averageTime = String.format("%.3f", (timeSum / 10));
            out.println(driver.getLargestWCC() + "/" + averageTime);
        }
        catch (IOException e) {

        }

        System.exit(1);
    }
}