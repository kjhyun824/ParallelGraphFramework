import algorithm.diam.DIAMDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.DIAMPartition;

import java.util.concurrent.BrokenBarrierException;

/**
 * Created by junghyun on 2017. 5. 8..
 */
public class DIAMMain {
    public static void main(String[] args)
            throws BrokenBarrierException, InterruptedException {
        final boolean isDirected = false;
        final boolean isWeighted = true;

        if(args.length != 5) {
            System.err.println("Arguments : <InputFile> <numThreads> <sync> <numCheck> <expOfPartitionSize>");
            System.exit(1);
        }

        String inputFile = args[0];
        int numThreads = Integer.parseInt(args[1]);
        int delta = Integer.parseInt(args[2]);
        int sync = Integer.parseInt(args[3]);
        int numCheck = Integer.parseInt(args[4]);
        int expOfPartitionSize = Integer.parseInt(args[5]);

        int asyncRangeSize = ((1 << expOfPartitionSize) * sync);

        Graph<DIAMPartition> graph = Graph.getInstance(expOfPartitionSize,isDirected,isWeighted);
        System.out.println("[DEBUG] Graph Loading ... ");
        GraphUtil.load(graph,inputFile);
        graph.generatePartition(asyncRangeSize,DIAMPartition.class);

        DIAMDriver driver = new DIAMDriver(graph, numThreads, delta, numCheck);

        final int numRun = 20;
        long[] elapsedTime = new long[numRun];
        double timeSum = 0;

        System.out.println("[DEBUG] Diameter Running ... ");

        long start;
        for(int i = 0; i < numRun; i++) {
            driver.reset();
            start = System.currentTimeMillis();
            driver.run();
            elapsedTime[i] = System.currentTimeMillis() - start;
            System.out.println("[DEBUG] elapsed time for iteration" + i + " : " + ((elapsedTime[i]) / 1000.0));

            if (i >= 10) {
                timeSum += (elapsedTime[i] / 1000.0);
            }
            driver.printDiameter();
        }

        System.out.println("[DEBUG] Diameter End");

        String averageTime = String.format("%.3f", (timeSum/10));
        System.out.println(averageTime);

        System.exit(1);
    }
}
