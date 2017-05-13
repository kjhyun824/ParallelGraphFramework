package algorithm.pagerank.original;

import graph.Graph;
import graph.Node;
import graph.sharedData.PageRankSharedData;
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
public class PageRankDriver
{
    int numThreads;
    int numTasks;
    int taskSize;
    int iteration;
    int nodeCapacity;
    double dampingFactor;

    Graph<PageRankSharedData> graph;
    PageRankSharedData sharedDataObject;
    DoubleBinaryOperator updateFunction;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;
    CyclicBarrier exitBarriers;

    Task[] initTasks;
    Task[] workTasks;
    Task[] barrierTasks;
    Task[] exitBarrierTasks;

    public PageRankDriver(Graph<PageRankSharedData> graph, double dampingFactor, int numThreads, int iteration) {
        this.graph = graph;
        this.dampingFactor = dampingFactor;
        this.numThreads = numThreads;
        this.taskSize = 1 << graph.getExpOfTaskSize();
        this.iteration = iteration;
        sharedDataObject = graph.getSharedDataObject();
        nodeCapacity = graph.getMaxNodeId() + 1;
        numTasks = (nodeCapacity + taskSize - 1) / taskSize;
        init();
    }

    public void init() {
        updateFunction = (prev, value) -> prev + value;
        PageRankSharedData.setUpdateFunction(updateFunction);

        initTasks = new Task[numTasks];
        workTasks = new Task[numTasks];
        barrierTasks = new Task[numThreads];
        exitBarrierTasks = new Task[numThreads];

        barriers = new CyclicBarrier(numThreads);
        exitBarriers = new CyclicBarrier(numThreads + 1);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numTasks; i++) {
            int beginRange = i * taskSize;
            int endRange = beginRange + taskSize;

            if (endRange > nodeCapacity) {
                endRange = nodeCapacity;
            }

            initTasks[i] = new Task(new PageRankInit(beginRange, endRange, graph, dampingFactor));
            workTasks[i] = new Task(new PageRankExecutor(beginRange, endRange, graph, dampingFactor));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new BarrierTask(barriers));
            exitBarrierTasks[i] = new Task(new BarrierTask(exitBarriers));
        }
    }

    public void run() throws BrokenBarrierException, InterruptedException {
        boolean isFirst = true;
        for (int i = 0; i < iteration; i++) {
            pushTasks(initTasks);
            pushTasks(exitBarrierTasks);
            exitBarriers.await();
            if (!isFirst) {
                sharedDataObject.initializedCallback();
            }
            pushTasks(workTasks);
            pushTasks(barrierTasks);
            isFirst = false;
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
        sharedDataObject.reset();
    }

    public String _printPageRankSum() {
        ArrayList<Double> pageRankValues = new ArrayList<>();
        double sum = 0.0d;

        sharedDataObject.initializedCallback();
        for (int j = 0; j < nodeCapacity; j++) {
            Node node = graph.getNode(j);
            if (node == null) {
                continue;
            }
            pageRankValues.add(sharedDataObject.getVertexValue(node.getInDegree(), j));
            sum += sharedDataObject.getVertexValue(node.getInDegree(), j);
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
            int nodeId = sampleData[i];
            pageRank[i] = sharedDataObject.getVertexValue(graph.getNode(nodeId).getInDegree(), nodeId);

//            System.out.println(sampleData[i] + " : " + graph.getNode(node).getInDegree() + " : " + graph.getNode(node).getOutDegree());
        }
        return pageRank;
    }
}

