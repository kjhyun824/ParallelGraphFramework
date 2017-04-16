package algorithm.wcc;

import graph.Graph;
import graph.partition.WCCPartition;
import task.*;
import thread.*;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.IntBinaryOperator;

public class WCCDriver {
    static final int ACTIVE = 1;
    static final int IN_ACTIVE = 0;

    int numThreads;
    boolean isDone;

    Graph<WCCPartition> graph;
    IntBinaryOperator updateFunction;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    Task[] fwTraverseStartTasks;
    Task[] fwTraverseRestTasks;
    Task[] barrierTasks;

    int[] isPartitionActives;

    public WCCDriver(Graph<WCCPartition> graph, int numThreads) {
        this.graph = graph;
        this.numThreads = numThreads;
        isDone = false;

        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();

        updateFunction = getUpdateFunction();
        WCCPartition.setUpdateFunction(updateFunction);

        fwTraverseStartTasks = new Task[numPartitions];
        fwTraverseRestTasks = new Task[numPartitions];
        isPartitionActives = new int[numPartitions];
        barrierTasks = new Task[numThreads];

        barriers = new CyclicBarrier(numThreads);
        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            fwTraverseStartTasks[i] = new Task(new WCCForwardTraversalStart(i, graph));
            fwTraverseRestTasks[i] = new Task(new WCCForwardTraversalRest(i, graph));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new TaskBarrier(barriers));
        }
    }

    public void run() {
        runAllTasksOnce(fwTraverseStartTasks);
        runAllTasksOnce(barrierTasks);
        runAllTasksOnce(fwTraverseRestTasks);

        while (!isDone) {
            runAllTasksOnce(barrierTasks);
            busyWaitForSyncStopMilli(10);
            runSomeTasksOnce(fwTraverseRestTasks);
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

    public void runSomeTasksOnce(Task[] tasks) {
        int count = 0;

        for (int i = 0; i < tasks.length; i++) {
            if (graph.getPartition(i).checkPartitionIsActive(ACTIVE)) {
                taskQueue.offer(tasks[i]);
                count++;
            }
        }

        if (count == 0) {
            isDone = true;
        }
    }

    public void runAllTasksOnce(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.offer(tasks[i]);
        }
    }

    public int getLargestWCC() {
        WCCPartition[] partitions = graph.getPartitions();
        int[] colors = new int[graph.getMaxNodeId() + 1];
        for (int i = 0; i < partitions.length; i++) {
            for (int j = 0; j < partitions[i].getSize(); j++) {
                int color = partitions[i].getVertexValue(j);
                colors[color]++;
            }
        }
        int max = 0;
        for (int i = 0; i < graph.getMaxNodeId() + 1; i++) {
            max = Math.max(max, colors[i]);
        }
        return max;
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

    public void reset() {
        for (int i = 0; i < graph.getNumPartitions(); i++) {
            graph.getPartition(i).reset();
        }
        isDone = false;
    }
}
