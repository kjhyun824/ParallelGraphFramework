package algorithm.wcc;

import graph.Graph;
import graph.partition.WCCPartition;
import task.*;
import thread.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class WCCDriver {
    static volatile int currentEpoch;
    final int numThreads;
    final int seed;
    final int initPart;
    final int numCheck;

    boolean isDone;

    final Graph<WCCPartition> graph;
    ConcurrentLinkedQueue<Task> taskQueue;
    ConTaskWaitingRunnable runnable;

    CyclicBarrier barriers;

    Task[] workerTasks;
    Task[] barrierTasks;

    static AtomicInteger before, after;

    public WCCDriver(Graph<WCCPartition> graph, int numThreads, int seed, int numCheck) {
        this.graph = graph;
        this.numThreads = numThreads;
        this.seed = seed;
        this.numCheck = numCheck;
        isDone = false;
        currentEpoch = 1;

        before = new AtomicInteger(0);
        after = new AtomicInteger(0);

//        initPart = graph.getNumPartitions() / seed;
        initPart = 1;
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
            workerTasks[i] = new Task(new WCCExecutor(i, graph, initPart, numCheck));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new BarrierTask(barriers));
        }
    }

    public void run() throws BrokenBarrierException, InterruptedException {
        pushAllTasks(workerTasks);
        pushAllTasks(barrierTasks);
        barriers.await();
        while (!isDone) {
            currentEpoch++;
            isDone = pushSomeTasks(workerTasks);
            pushAllTasks(barrierTasks);
            barriers.await();
        }
        /*
        int count = 0;
        WCCExecutor.setIsFront(true);
        pushFrontTasks(workerTasks,initPart);
        pushAllTasks(barrierTasks);
        barriers.await();
        count++;
        while (!isDone) {
            currentEpoch++;
            isDone = pushSomeTasks(workerTasks);
            pushAllTasks(barrierTasks);
            barriers.await();
            count++;
        }
        System.out.println("[DEBUG] Num Iter on Front : " + count);
        System.out.println("[DEBUG] before / after : " + before + " / " + after);
        count = 0;

        WCCExecutor.setIsFront(false);
        pushBackTasks(workerTasks,initPart);
        pushAllTasks(barrierTasks);
        barriers.await();
        count++;
        while (!isDone) {
            currentEpoch++;
            isDone = pushSomeTasks(workerTasks);
            pushAllTasks(barrierTasks);
            barriers.await();
            count++;
        }
        System.out.println("[DEBUG] Num Iter on Back : " + count);
//        System.out.println("[DEBUG] before / after : " + before + " / " + after);
        */
    }

    public void print() {
        String fileName = "WCC_Thr" + numThreads + "_Check" + numCheck + ".txt";
        try (FileWriter fw = new FileWriter(fileName, true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
            for (int i = 0; i < graph.getNumPartitions(); i++) {
                WCCPartition partition = graph.getPartition(i);
                int offset = i << graph.getExpOfPartitionSize();
//            for (int j = 0; j < partition.getSize(); j++) {
//                int nodeId = offset + j;
//                String distance = String.format("%.3f", partition.getVertexValue(j));
//                System.out.println(nodeId + "   " + distance);
//            }

                for (int j = 0; j < partition.getSize(); j++) {
                    int nodeId = offset + j;
                    int dist = partition.getNextCompId(j);
                    if (dist == Integer.MAX_VALUE) {
                        dist = -1;
                    }
                    out.println(nodeId + "," + dist);
                }
            }
        } catch (IOException e) {

        }
    }

    public void pushFrontTasks(Task[] tasks, int numTasks) {
        for (int i = 0; i < numTasks; i++) {
            taskQueue.offer(tasks[i]);
        }
    }

    public void pushBackTasks(Task[] tasks, int numTasks) {
        for (int i = numTasks; i < tasks.length; i++) {
            taskQueue.offer(tasks[i]);
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

    public int getNumWCC() {
        WCCPartition[] partitions = graph.getPartitions();
        int count = 0;
        boolean[] compExists = new boolean[graph.getMaxNodeId() + 1];
        for (int i = 0; i < graph.getMaxNodeId() + 1; i++) {
            compExists[i] = false;
        }

        for (int i = 0; i < partitions.length; i++) {
            int offset = i << graph.getExpOfPartitionSize();
            for (int j = 0; j < partitions[i].getSize(); j++) {

                if (graph.getNode(offset + j) == null) {
                    continue;
                }

                int compId = partitions[i].getNextCompId(j);
                if (!compExists[compId]) {
                    compExists[compId] = true;
                }
            }
        }

        for (int i = 0; i < graph.getMaxNodeId() + 1; i++) {
            if (compExists[i]) {
                count++;
            }
        }

        return count;
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

    public int getMinWCC() {
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

        int min = Integer.MAX_VALUE;
        for (int i = 0; i < graph.getMaxNodeId() + 1; i++) {
            min = Math.min(min, compIdCount[i]);
        }
        return min;
    }

    public void reset() {
        for (int i = 0; i < graph.getNumPartitions(); i++) {
            graph.getPartition(i).reset();
        }
        isDone = false;
        currentEpoch = 1;
//        before.set(0);
//        after.set(0);
    }

    public static int getCurrentEpoch() {
        return currentEpoch;
    }

    public static void incBefore() {
        int temp;
        do {
            temp = before.get();
        } while (!before.compareAndSet(temp, temp + 1));
    }

    public static void incAfter() {
        int temp;
        do {
            temp = after.get();
        } while (!after.compareAndSet(temp, temp + 1));
    }
}
