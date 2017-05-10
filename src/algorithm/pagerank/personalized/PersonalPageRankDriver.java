package algorithm.pagerank.personalized;

import graph.Graph;
import graph.partition.PersonalPageRankPartition;
import task.BarrierTask;
import task.Task;
import thread.TaskWaitingRunnable;
import thread.ThreadUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.DoubleBinaryOperator;

/**
 * PageRank Algorithm Implementation
 **/
public class PersonalPageRankDriver
{
    int numThreads;
    int iteration;
    double dampingFactor;
    int[] seedSet;
    int numSeeds;

    Graph<PersonalPageRankPartition> graph;
    DoubleBinaryOperator updateFunction;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;
    CyclicBarrier exitBarriers;

    Task[] initTasks;
    Task[] workTasks;
    Task[] barrierTasks;
    Task[] barrierTasks2;
    Task[] exitBarrierTasks;

    public PersonalPageRankDriver(Graph<PersonalPageRankPartition> graph, double dampingFactor, int iteration, int numThreads, String seedFile, int numSeeds) {
        this.graph = graph;
        this.dampingFactor = dampingFactor;
        this.iteration = iteration;
        this.numThreads = numThreads;
        this.numSeeds = numSeeds;
        seedFileRead(seedFile);
        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();

        updateFunction = (prev, value) -> prev + value;
        PersonalPageRankPartition.setUpdateFunction(updateFunction);

        initTasks = new Task[numPartitions];
        workTasks = new Task[numPartitions];
        barrierTasks = new Task[numThreads];
        barrierTasks2 = new Task[numThreads];
        exitBarrierTasks = new Task[numThreads];

        barriers = new CyclicBarrier(numThreads);
        exitBarriers = new CyclicBarrier(numThreads + 1);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            initTasks[i] = new Task(new PersonalPageRankInit(i, graph, dampingFactor, numSeeds));
            workTasks[i] = new Task(new PersonalPageRankExecutor(i, graph, dampingFactor));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new BarrierTask(barriers));
            barrierTasks2[i] = new Task(new BarrierTask(exitBarriers));
            exitBarrierTasks[i] = new Task(new BarrierTask(exitBarriers));
        }
    }

    public void run() throws BrokenBarrierException, InterruptedException {
        int numPartitions = graph.getNumPartitions();
        PersonalPageRankPartition[] partitions = graph.getPartitions();

        for (int i = 0; i < iteration; i++) {
            pushTasks(initTasks);
            pushTasks(barrierTasks);
            pushTasks(workTasks);
            pushTasks(barrierTasks2);
            exitBarriers.await();

            int activeNumNodes = 0;
            for (int j = 0; j < numPartitions; j++) {
                activeNumNodes += partitions[j].getActiveNumNodes();
            }
            PersonalPageRankInit.setActiveNumNodes(activeNumNodes);
        }
        pushTasks(exitBarrierTasks);
        exitBarriers.await();
    }

    public void pushTasks(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.add(tasks[i]);
        }
    }

    //For JIT Test
    public void reset() {

        for (int i = 0; i < initTasks.length; i++) {
            initTasks[i].reset();
        }

        for (int i = 0; i < numSeeds; i++) {
            int seed = seedSet[i];
            int partitionId = graph.getPartitionId(seed);
            int posInPart = graph.getNodePositionInPart(seed);
            graph.getPartition(partitionId).setActive(posInPart);
        }
    }

    public int getNumActiveNodes() {
        int activeNumNodes = 0;
        int numPartitions = graph.getNumPartitions();
        PersonalPageRankPartition[] partitions = graph.getPartitions();

        for (int j = 0; j < numPartitions; j++) {
            activeNumNodes += partitions[j].getActiveNumNodes();
        }
        return activeNumNodes;

    }

    public void seedFileRead(String seedFile) {
        seedSet = new int[numSeeds];
        Path path = Paths.get(seedFile);
        int i = 0;

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String seed;
            while ((seed = reader.readLine()) != null) {
                seedSet[i] = Integer.parseInt(seed);
                i++;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public double _printPageRankSum() {
        PersonalPageRankPartition[] partitions = graph.getPartitions();
        ArrayList<Double> pageRankValues = new ArrayList<>();
        double sum = 0.0d;

        for (int i = 0; i < partitions.length; i++) {
            partitions[i].initializedCallback();
            int partitionSize = partitions[i].getSize();
            for (int j = 0; j < partitionSize; j++) {
                pageRankValues.add(partitions[i].getVertexValue(j));
                sum += partitions[i].getVertexValue(j);
            }
        }

        Collections.sort(pageRankValues, Collections.reverseOrder());

//        for (int i = 0; i < 10; i++) {
//            System.err.println("pageRank " + i + " : " + pageRankValues.get(i));
//        }
//        String pageRanksum = String.format("%.3f", sum);

        return sum;
    }

    public double[] _getPageRank(int[] sampleData) {
        double[] pageRank = new double[sampleData.length];

        for (int i = 0; i < sampleData.length; i++) {
            int node = sampleData[i];
            int partitionNumber = graph.getPartitionId(node);
            int nodePosition = graph.getNodePositionInPart(node);

            PersonalPageRankPartition doublePartition = graph.getPartition(partitionNumber);
            pageRank[i] = doublePartition.getVertexValue(nodePosition);

//            System.out.println(sampleData[i] + " : " + graph.getNode(node).getInDegree() + " : " + graph.getNode(node).getOutDegree());
        }
        return pageRank;
    }
}

