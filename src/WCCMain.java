import algorithm.wcc.WCCDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.WCCPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class WCCMain
{
    public static void main(String[] args) {
        final boolean isDirected = false;
        final boolean isWeighted = false;
        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        double asyncPercent = Double.parseDouble(args[2]);

        int expOfPartitionSize = 16; // 1 << 16;
        int asyncRangeSize = (int) ((1 << 16) * asyncPercent);

        long start = System.currentTimeMillis();
        Graph<WCCPartition> graph = Graph.getInstance(expOfPartitionSize, isDirected, isWeighted);
        GraphUtil.load(graph, inputFile);
        graph.generatePartition(0, asyncRangeSize, WCCPartition.class);
        long loadingTime = System.currentTimeMillis() - start;

        System.out.println("[DEBUG] Graph Loading : " + ((double) loadingTime / 1000.0));

        WCCDriver driver = new WCCDriver(graph, numThreads);

        long[] elapsedTime = new long[20];

        for (int i = 0; i < 20; i++) {
            driver.reset();
            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;
//            wccValue[i] = driver.getLargestWCC();
            if (i >= 10) {
                System.out.println("[DEBUG] elapsed time for iteration" + (i - 10) + " : " + (((double) elapsedTime[i]) / (1000.0)));
            }
        }

        long timeSum = 0;
        long max = -1;
        long min = 1000000;

        for (int i = 0; i < elapsedTime.length - 10; i++) {
            max = Math.max(max, elapsedTime[i + 10]);
            min = Math.min(max, elapsedTime[i + 10]);
            System.out.print(elapsedTime[i + 10] / (double) 1000 + " ");
            timeSum += elapsedTime[i + 10];
        }
        timeSum -= max;
        timeSum -= min;

        double avg = timeSum / (double) 8;
        System.out.print("AVG : " + avg);


        try (FileWriter fw = new FileWriter(String.valueOf(asyncPercent) + "out.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
            out.print((avg / 1000) + "/");
        }
        catch (IOException e) {

        }

        System.exit(1);
    }
}