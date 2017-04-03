package algorithm.wcc;

import graph.DirectedGraph;
import graph.partition.IntegerPartition;
import task.*;
import thread.*;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.IntBinaryOperator;

public class WCCDriver {
    static final byte ACTIVE = 1;
    int numThreads;
    boolean isDone;

    DirectedGraph<IntegerPartition> graph;
    IntBinaryOperator updateFunction;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    Task[] fwTraverseStartTasks;
    Task[] fwTraverseStopTasks;
    Task[] barrierTasks;

    public WCCDriver(DirectedGraph<IntegerPartition> graph, int numThreads) {
        this.graph = graph;
        this.numThreads = numThreads;
        isDone = false;

        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();

        updateFunction = getUpdateFunction();
        IntegerPartition.setUpdateFunction(updateFunction);

        fwTraverseStartTasks = new Task[numPartitions];
        fwTraverseStopTasks = new Task[numPartitions];

        barriers = new CyclicBarrier(numThreads);
        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            fwTraverseStartTasks[i] = new Task(i, new WCCForwardTraversalStart(graph));
            fwTraverseStopTasks[i] = new Task(i, new WCCForwardTraversalRest(graph));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(i, new TaskBarrier(i, barriers));
        }
    }

    public void run() {
        runAllTasksOnce(fwTraverseStartTasks);

        while (true) {
            runSomeTasksOnce(fwTraverseStopTasks);
            if (isDone) {
                break;
            }
            runAllTasksOnce(barrierTasks);
            busyWaitForSyncStopMilli(10);
        }

        IntegerPartition partition = graph.getPartition(0);
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

    public void runSomeTasksOnce(Task[] tasks) {
        IntegerPartition[] partitions = graph.getPartitions();
        for (int i = 0; i < tasks.length; i++) {
            if (partitions[i].checkPartitionIsActive(ACTIVE)) {
                taskQueue.offer(tasks[i]);
            }
        }
    }

    public void runAllTasksOnce(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.offer(tasks[i]);
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

    public void reset() {

    }
}
