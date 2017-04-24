package algorithm.wcc;

import graph.Graph;
import graph.partition.WCCPartition;
import task.*;
import thread.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;

public class WCCDriver
{
    static final int ACTIVE = 1;
    final int numThreads;

    boolean isDone;

    final Graph<WCCPartition> graph;
    //    LinkedBlockingQueue<Task> taskQueue;
    ConcurrentLinkedQueue<Task> taskQueue;
    //    TaskWaitingRunnable runnable;
    ConTaskWaitingRunnable runnable;

    CyclicBarrier barriers;

    Task[] workerTasks;
    Task[] barrierTasks;

    public WCCDriver(Graph<WCCPartition> graph, int numThreads) {
        this.graph = graph;
        this.numThreads = numThreads;
        isDone = false;

        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();

        workerTasks = new Task[numPartitions];
        barrierTasks = new Task[numThreads];

        barriers = new CyclicBarrier(numThreads + 1);
        taskQueue = new ConcurrentLinkedQueue<>();
        runnable = new ConTaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            workerTasks[i] = new Task(new WCCExecutor(i, graph));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new BarrierTask(barriers));
        }
    }

    public void run() throws BrokenBarrierException, InterruptedException {
        pushAllTasks(workerTasks);
        while (true) {
            pushAllTasks(barrierTasks);
            barriers.await();
            pushSomeTasks(workerTasks);
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
        int[] compIdCount = new int[graph.getMaxNodeId() + 1];
        for (int i = 0; i < partitions.length; i++) {
            for (int j = 0; j < partitions[i].getSize(); j++) {
                int compId = partitions[i].getNextCompId(j);
                compIdCount[compId]++;
            }
        }

        int max = 0;
        for (int i = 0; i < graph.getMaxNodeId() + 1; i++) {
            max = Math.max(max, compIdCount[i]);
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