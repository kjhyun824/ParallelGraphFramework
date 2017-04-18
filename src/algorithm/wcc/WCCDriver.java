package algorithm.wcc;

import graph.Graph;
import graph.partition.WCCPartition;
import task.*;
import thread.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

public class WCCDriver {
    static final int ACTIVE = 1;

    int numThreads;
    boolean isDone;

    Graph<WCCPartition> graph;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;
    CyclicBarrier barriers2;

    Task[] fwTraverseStartTasks;
    Task[] fwTraverseRestTasks;
    Task[] barrierTasks;
    Task[] barrierTasks2;

    int[] isPartitionActives;

    public WCCDriver(Graph<WCCPartition> graph, int numThreads) {
        this.graph = graph;
        this.numThreads = numThreads;
        isDone = false;

        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();

        fwTraverseStartTasks = new Task[numPartitions];
        fwTraverseRestTasks = new Task[numPartitions];
        isPartitionActives = new int[numPartitions];
        barrierTasks = new Task[numThreads];
        barrierTasks2 = new Task[numThreads];

        barriers = new CyclicBarrier(numThreads);
        barriers2 = new CyclicBarrier(numThreads + 1);
        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            fwTraverseStartTasks[i] = new Task(new WCCForwardTraversalStart(i, graph));
            fwTraverseRestTasks[i] = new Task(new WCCForwardTraversalRest(i, graph));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new TaskBarrier(barriers));
            barrierTasks2[i] = new Task(new TaskBarrier(barriers2));
        }
    }

    public void run()
            throws BrokenBarrierException, InterruptedException {
        runAllTasksOnce(fwTraverseStartTasks);
        runAllTasksOnce(barrierTasks);
        runAllTasksOnce(fwTraverseRestTasks);

        while (!isDone) {
            runAllTasksOnce(barrierTasks2);

            barriers2.await();
            barriers.reset();
            barriers2.reset();
            runSomeTasksOnce(fwTraverseRestTasks);
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

    public void reset() {
        for (int i = 0; i < graph.getNumPartitions(); i++) {
            graph.getPartition(i).reset();
        }
        isDone = false;
    }
}
