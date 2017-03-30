package algorithm.bfs;

import graph.DirectedGraph;
import graph.GraphPartition;
import graph.Node;
import graph.NodePartition;
import task.*;
import thread.*;
import util.list.TIntLinkedListQueue;

import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleBinaryOperator;

public class BFSDriver {
    static AtomicInteger numTotalActiveNodes;
    int numThreads;
    DirectedGraph graph;
    GraphPartition graphPartition;
    DoubleBinaryOperator updateFunction;
    LinkedBlockingQueue<DoubleTask> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    DoubleTask[] workTasks;
    DoubleTask[] barrierTasks;

    int level;

    public BFSDriver(DirectedGraph graph, int numThreads) {
        this.graph = graph;
        this.numThreads = numThreads;
        this.numTotalActiveNodes = new AtomicInteger(0);
        graphPartition = graph.getPartitionInstance();
        init();
    }

    public void init() {
        GraphPartition graphPartition = graph.getPartitionInstance();
        int numPartitions = graphPartition.getNumPartitions();
        level = 1;

        updateFunction = getUpdateFunction();
        NodePartition.setUpdateFunction(updateFunction);

        workTasks = new DoubleTask[numPartitions];
        barrierTasks = new DoubleTask[numThreads];
        barriers = new CyclicBarrier(numThreads);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            workTasks[i] = new DoubleTask(i, new BFSExecutor(graph));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new DoubleTask(i, new TaskBarrier(i, barriers));
        }
    }

    public void run() {
        After3level(); // Sequential Process
        BFSExecutor.setLevel(level);

        while (numTotalActiveNodes.get() > 0) {
            resetTotalActiveNodes();
            runOnce(workTasks);
            runOnce(barrierTasks);
            busyWaitForSyncStopMilli(10);

            level++;
            BFSExecutor.setLevel(level);
        }
    }

    public static void resetTotalActiveNodes() {
        numTotalActiveNodes.set(0);
    }

    public static int addTotalActiveNodes(int numNodes) {
        return numTotalActiveNodes.addAndGet(numNodes);
    }

    public void runOnce(DoubleTask[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.add(tasks[i]);
        }
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

    public DoubleBinaryOperator getUpdateFunction() {
        DoubleBinaryOperator updateFunction = (prev, value) -> value;
        return updateFunction;
    }
    //TODO : Partition structure need to change (phisical -> locgical)

    public void After3level() {
        int startNodeId = 0;
        NodePartition[] partitions = graphPartition.getPartitions();
        partitions[0].setVertexValue(0, level);

        TIntLinkedListQueue activeQueue = new TIntLinkedListQueue();
        activeQueue.add(startNodeId);

        for (int i = 0; i < 3; i++) {
            int count = 0;
            int curQueueSize = activeQueue.size();

            while (count < curQueueSize) {
                int activeNodeId = activeQueue.poll();
                Node node = graph.getNode(activeNodeId);
                int neighborListSize = node.neighborListSize();

                for (int j = 0; j < neighborListSize; j++) {
                    int destId = node.getNeighbor(j);
                    int destPartitionId = graphPartition.getPartitionId(destId);
                    int destPositionInPart = graphPartition.getNodePositionInPart(destId);
                    double destLevel = partitions[destPartitionId].getVertexValue(destPositionInPart);

                    if (destLevel == 0) {
                        partitions[destPartitionId].update(destPositionInPart, level + 1);
                        activeQueue.add(destId);
                    }
                }
                count++;
            }
            level++;
        }
    }

    public void reset() {
        for (int i = 0; i < graphPartition.getNumPartitions(); i++) {
            graphPartition.getPartition(i).reset();
        }
        level = 1;
    }
}