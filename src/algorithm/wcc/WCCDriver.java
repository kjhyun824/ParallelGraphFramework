package algorithm.wcc;

import graph.Graph;
import graph.partition.WCCPartition;
import task.*;
import thread.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

public class WCCDriver
{
    static final int ACTIVE = 1;
    final int numThreads;

    boolean isDone;

    Graph<WCCPartition> graph;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;
    CyclicBarrier barriers2;

    Task[] fwTraverseStartTasks;
    Task[] fwTraverseRestTasks;
    Task[] barrierTasks;
    Task[] barrier2Tasks;

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
        barrierTasks = new Task[numThreads];
        barrier2Tasks = new Task[numThreads];

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
            barrierTasks[i] = new Task(new BarrierTask(barriers));
            barrier2Tasks[i] = new Task(new BarrierTask(barriers2));
        }
    }

    public void run()
            throws BrokenBarrierException, InterruptedException {
        pushAllTasks(fwTraverseStartTasks);
        pushAllTasks(barrierTasks);
        pushAllTasks(fwTraverseRestTasks);

        while (true) {
            pushAllTasks(barrier2Tasks);
            barriers2.await();
            pushSomeTasks(fwTraverseRestTasks);
            if (isDone) {
                break;
            }
        }
    }

    public void pushSomeTasks(Task[] tasks) {
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

    public void pushAllTasks(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.offer(tasks[i]);
        }
    }

    public int getLargestWCC() {
        WCCPartition[] partitions = graph.getPartitions();
        int[] compIds = new int[graph.getMaxNodeId() + 1];
        for (int i = 0; i < partitions.length; i++) {
            for (int j = 0; j < partitions[i].getSize(); j++) {
                int compId = partitions[i].getNextCompId(j);
                compIds[compId]++;
            }
        }

        int max = 0;
        for (int i = 0; i < graph.getMaxNodeId() + 1; i++) {
            max = Math.max(max, compIds[i]);
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
