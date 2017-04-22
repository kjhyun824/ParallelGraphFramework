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
    static volatile int currentEpoch;
    //    static int currentEpoch;
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
        currentEpoch = 1;

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
        boolean isDone = false;
        pushAllTasks(workerTasks);
        while (!isDone) {
            pushAllTasks(barrierTasks);
            barriers.await();
            currentEpoch++;
            isDone = pushSomeTasks(workerTasks);
        }
    }

    public boolean pushSomeTasks(Task[] tasks) {
        int count = 0;

        for (int i = 0; i < tasks.length; i++) {
            if (graph.getPartition(i).getUpdatedEpoch() >= (currentEpoch - 1)) {
                taskQueue.offer(tasks[i]);
                count++;
            }
        }

        if (count == 0) {
            return true;
        }
        return false;
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
            int offset = i << graph.getExpOfPartitionSize();
            for (int j = 0; j < partitions[i].getSize(); j++) {

                if (graph.getNode(offset + j) == null) {
                    continue;
                }

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
        currentEpoch = 1;
    }

    public static int getCurrentEpoch() {
        return currentEpoch;
    }
}
