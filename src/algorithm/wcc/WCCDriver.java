package algorithm.wcc;

import graph.Graph;
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

    Graph<IntegerPartition> graph;
    IntBinaryOperator updateFunction;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    Task[] fwTraverseStartTasks;
    Task[] fwTraverseRestTasks;
    Task[] barrierTasks;

    byte[] isPartitionActives;

    public WCCDriver(Graph<IntegerPartition> graph, int numThreads) {
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
        fwTraverseRestTasks = new Task[numPartitions];
        isPartitionActives = new byte[numPartitions];
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
        runAllTasksOnce(fwTraverseRestTasks);

        while (true) {
            runAllTasksOnce(barrierTasks);
            busyWaitForSyncStopMilli(10);

            // get PartitionActiveValue;
            for (int i = 0; i < isPartitionActives.length; i++) {
                IntegerPartition partition = graph.getPartition(i);
                isPartitionActives[i] = graph.getPartition(i).getActiveValue();
                partition.setPartitionActiveValue((byte) 0);
            }

            runSomeTasksOnce(fwTraverseRestTasks);
            if (isDone) {
                break;
            }
        }

        IntegerPartition[] partitions = graph.getPartitions();
        int count = 0;

        for (int i = 0; i < partitions.length; i++) {
            for (int j = 0; j < partitions[i].getSize(); j++) {
                int color = partitions[i].getVertexValue(j);
                if (color == 4847570) {
                    count++;
                } else {
                    System.out.println("NOT 4847570 SET : " + color);
                }
            }
        }

        System.out.println("4847570 SET COUNT : " + count);
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
            if (isPartitionActives[i] == 1) {
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
