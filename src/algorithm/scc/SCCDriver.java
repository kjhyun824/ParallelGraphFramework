package algorithm.scc;

import graph.DirectedGraph;
import graph.partition.IntegerGraphPartition;
import graph.partition.IntegerNodePartition;
import task.Task;
import task.TaskBarrier;
import thread.TaskWaitingRunnable;
import thread.ThreadUtil;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleBinaryOperator;
import java.util.function.IntBinaryOperator;

public class SCCDriver {
    int numThreads;

    DirectedGraph<IntegerGraphPartition> graph;
    IntegerGraphPartition graphPartition;
    IntBinaryOperator updateFunction;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    Task[] trimTasks;
    Task[] fwTraverseStartTasks;
    Task[] fwTraverseStopTasks;
    Task[] bwTraverseTasks;
    Task[] barrierTasks;

    boolean[] isInActiveNode;

    public SCCDriver(DirectedGraph<IntegerGraphPartition> graph, int numThreads) {
        this.graph = graph;
        this.numThreads = numThreads;
        graphPartition = graph.getPartitionInstance();
        isInActiveNode = new boolean[graph.getMaxNodeId() + 1];

        init();
    }

    public void init() {
        int numPartitions = graphPartition.getNumPartitions();
        graph.generateTransposeEdges(); // G^t = (V, E^T)

        updateFunction = getUpdateFunction();
        IntegerNodePartition.setUpdateFunction(updateFunction);

        trimTasks = new Task[numPartitions];
        fwTraverseStartTasks = new Task[numPartitions];
        fwTraverseStopTasks = new Task[numPartitions];
        bwTraverseTasks = new Task[numPartitions];
        barrierTasks = new Task[numPartitions];

        barriers = new CyclicBarrier(numThreads);
        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            trimTasks[i] = new Task(i, new SCCTrim(graph, isInActiveNode));
            fwTraverseStartTasks[i] = new Task(i, new SCCForwardTraversalStart(graph));
            fwTraverseStopTasks[i] = new Task(i, new SCCForwardTraversalRest(graph, isInActiveNode));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(i, new TaskBarrier(i, barriers));
        }
    }

    public void run() {
        runOnce(trimTasks);
        runOnce(fwTraverseStartTasks);
        for (int i = 0; i < 5; i++) {
            runOnce(fwTraverseStopTasks);
            runOnce(barrierTasks);
        }

        busyWaitForSyncStopMilli(10);

        for (int i = 0; i < isInActiveNode.length; i++) {
            if (isInActiveNode[i] == true) {
                System.out.print(" " + i);
            }
        }

        System.out.println();
        IntegerNodePartition partition = graphPartition.getPartition(0);
        for (int i = 0; i < partition.getSize(); i++) {
            System.out.print(" " + partition.getVertexValue(i));
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

    public void runOnce(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.add(tasks[i]);
        }
    }

    public IntBinaryOperator getUpdateFunction() {
        IntBinaryOperator updateFunction = (prev, value) -> {
            int updateValue = prev;
            if (prev < value) {
                updateValue = value;
            }
            return updateValue;
        };
        return updateFunction;
    }
}

//
//    public void doFwStart() {
//        int maxNodeId = graphT.getMaxNodeId();
//
//        for (int i = 0; i <= maxNodeId; i++) {
//            if (!isInActiveNode[i]) {
//                Node node = graphT.getNode(i);
//                if (node != null) {
//                    node.setColorId(i);
//                    sendOwnIdToOutNeighbor(node);
//                }
//            }
//        }
//    }
//
//    public void doFwRest() {
//        int maxNodeId = graphT.getMaxNodeId();
//
//        for (int i = 0; i <= maxNodeId; i++) {
//            if (!isInActiveNode[i]) {
//                Node node = graphT.getNode(i);
//                if (node != null) {
//
//                }
//            }
//        }
//    }
//
//    public void sendOwnIdToOutNeighbor(Node node) {
//        int colorId = node.getColorId();
//        int neighborListSize = node.neighborListSize();
//
//        for (int i = 0; i < neighborListSize; i++) {
//            int neighborId = node.getNeighbor(i);
//            Node neighborNode = graph.getNode(neighborId);
//            neighborNode.addColorId(colorId);
//        }
//    }
//}