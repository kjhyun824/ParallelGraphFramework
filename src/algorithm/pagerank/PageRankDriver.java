package algorithm.pagerank;

import graph.Graph;
import graph.partition.PageRankPartition;
import task.*;
import thread.TaskWaitingRunnable;
import thread.ThreadUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.DoubleBinaryOperator;

/**
 * PageRank Algorithm Implementation
 **/
public class PageRankDriver {
    int numThreads;
    int iteration;
    double dampingFactor;

    Graph<PageRankPartition> graph;
    DoubleBinaryOperator updateFunction;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;
    CyclicBarrier exitBarriers;

    Task[] initTasks;
    Task[] workTasks;
    Task[] barrierTasks;
    Task[] exitBarrierTasks;
    Task[] barrierResetTasks;

    public PageRankDriver(Graph<PageRankPartition> graph, double dampingFactor, int iteration, int numThreads) {
        this.graph = graph;
        this.dampingFactor = dampingFactor;
        this.iteration = iteration;
        this.numThreads = numThreads;
        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();

        updateFunction = getUpdateFunction();
        PageRankPartition.setUpdateFunction(updateFunction);

        initTasks = new Task[numPartitions];
        workTasks = new Task[numPartitions];
        barrierTasks = new Task[numThreads];
        exitBarrierTasks = new Task[numThreads];
        barrierResetTasks = new Task[1];

        barriers = new CyclicBarrier(numThreads);
        exitBarriers = new CyclicBarrier(numThreads + 1);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            initTasks[i] = new Task(new PageRankInit(i, graph, dampingFactor));
            workTasks[i] = new Task(new PageRankExecutor(i, graph, dampingFactor));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new BarrierTask(barriers));
            exitBarrierTasks[i] = new Task(new BarrierTask(exitBarriers));
        }
        barrierResetTasks[0] = new Task(new BarrierResetTask(barriers));
    }

    public void run()
            throws BrokenBarrierException, InterruptedException {
        for (int i = 0; i < iteration; i++) {
            pushTasks(initTasks);
            pushTasks(barrierTasks);
            pushTasks(barrierResetTasks);
            pushTasks(workTasks);
            pushTasks(barrierTasks);
            pushTasks(barrierResetTasks);
        }
        pushTasks(exitBarrierTasks);
        exitBarriers.await();
    }

    public void pushTasks(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.add(tasks[i]);
        }
    }

    public DoubleBinaryOperator getUpdateFunction() {
        DoubleBinaryOperator updateFunction = (prev, value) -> prev + value;
        return updateFunction;
    }

    //For JIT Test
    public void reset() {
        exitBarriers.reset();
        for (int i = 0; i < initTasks.length; i++) {
            initTasks[i].reset();
        }
    }

    public String _printPageRankSum() {
        PageRankPartition[] partitions = graph.getPartitions();
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
//            System.out.println("[DEBUG] pageRank " + i + " : " + pageRankValues.get(i));
//        }
        String pageRanksum = String.format("%.3f", sum);

        return pageRanksum;
    }


    public double[] _getPageRank(int[] sampleData) {
        double[] pageRank = new double[sampleData.length];

        for (int i = 0; i < sampleData.length; i++) {
            int node = sampleData[i];
            int partitionNumber = graph.getPartitionId(node);
            int nodePosition = graph.getNodePositionInPart(node);

            PageRankPartition doublePartition = graph.getPartition(partitionNumber);
            pageRank[i] = doublePartition.getVertexValue(nodePosition);

//            System.out.println(sampleData[i] + " : " + graph.getNode(node).getInDegree() + " : " + graph.getNode(node).getOutDegree());
        }
        return pageRank;
    }
}

