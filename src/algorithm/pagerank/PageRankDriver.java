package algorithm.pagerank;

import graph.Graph;
import graph.partition.PageRankPartition;
import task.*;
import thread.TaskWaitingRunnable;
import thread.ThreadUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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

    Task[] initTasks;
    Task[] workTasks;
    Task[] barrierTasks;

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
        barriers = new CyclicBarrier(numThreads);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            initTasks[i] = new Task(new PageRankInit(i, graph, dampingFactor));
            workTasks[i] = new Task(new PageRankExecutor(i, graph, dampingFactor));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new TaskBarrier(barriers));
        }
    }

    public void run() {
        for (int i = 0; i < iteration; i++) {
            runOnce(initTasks);
            runOnce(barrierTasks);
            runOnce(workTasks);
            runOnce(barrierTasks);
        }
        busyWaitForSyncStopMilli(10);
    }

    public void busyWaitForSyncStopMilli(int millisecond) {
        while (taskQueue.size() != 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(millisecond);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void runOnce(Task[] tasks) {
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
        for (int i = 0; i < initTasks.length; i++) {
            initTasks[i].reset();
        }
    }

    public void _printPageRankSum() {
        PageRankPartition[] partitions = graph.getPartitions();
        ArrayList<Double> pagerank = new ArrayList<>();
        double sum = 0.0d;

        for (int i = 0; i < partitions.length; i++) {
            partitions[i].initializedCallback();
            int numNodeInPart = partitions[i].getSize();
            for (int j = 0; j < numNodeInPart; j++) {
                pagerank.add(partitions[i].getVertexValue(j));
                sum += partitions[i].getVertexValue(j);
            }
        }

        Collections.sort(pagerank, Collections.reverseOrder());

        for (int i = 0; i < 10; i++) {
            System.out.println("pageRank : " + i + " : " + pagerank.get(i));
        }
        System.out.println("PageRank : " + sum);
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

