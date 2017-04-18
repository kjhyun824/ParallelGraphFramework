package algorithm.sssp;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import graph.Graph;
import graph.Node;
import graph.partition.SSSPPartition;
import task.Task;
import task.TaskBarrier;
import thread.SSSPTaskWaitingRunnable;
import thread.TaskWaitingRunnable;
import thread.ThreadUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SSSPDriver
{
    int numThreads;
    double delta;
    boolean lightIsDone;

    static int innerIdx;
    static int bucketIdx;

    static TIntArrayList[] lightEdges;
    static TDoubleArrayList[] lightWeights;
    static TIntArrayList[] heavyEdges;
    static TDoubleArrayList[] heavyWeights;

    Graph<SSSPPartition> graph;
    LinkedBlockingQueue<Task> taskQueue;
    SSSPTaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    Task[] workTasks;
    Task[] barrierTasks;

    SSSPExecutor[] ssspExecutors;

    ReentrantLock lock = new ReentrantLock();
    Condition condition = lock.newCondition();

    public SSSPDriver(Graph<SSSPPartition> graph, int numThreads, double delta, int source) {
        this.graph = graph;
        this.numThreads = numThreads;
        this.delta = delta;
        this.lightIsDone = false;
        this.bucketIdx = 0;
        this.innerIdx = 1;

        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();

        workTasks = new Task[numPartitions];
        ssspExecutors = new SSSPExecutor[numPartitions];
        barrierTasks = new Task[numThreads];

        barriers = new CyclicBarrier(numThreads + 1);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new SSSPTaskWaitingRunnable(taskQueue, lock, condition);

        int numNodes = graph.getNumNodes();
        lightEdges = new TIntArrayList[numNodes];
        lightWeights = new TDoubleArrayList[numNodes];
        heavyEdges = new TIntArrayList[numNodes];
        heavyWeights = new TDoubleArrayList[numNodes];

        for (int i = 0; i < numNodes; i++) {
            lightEdges[i] = new TIntArrayList();
            lightWeights[i] = new TDoubleArrayList();
            heavyEdges[i] = new TIntArrayList();
            heavyWeights[i] = new TDoubleArrayList();

            Node srcNode = graph.getNode(i);

            for (int j = 0; j < srcNode.getOutDegree(); j++) {
                int destId = srcNode.getNeighbor(j);
                double weight = srcNode.getWeight(destId);
                if (weight > delta) {
                    heavyEdges[i].add(destId);
                    heavyWeights[i].add(weight);
                }
                else {
                    lightEdges[i].add(destId);
                    lightWeights[i].add(weight);
                }
            }
        }

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            ssspExecutors[i] = new SSSPExecutor(i, graph, delta);
            workTasks[i] = new Task(ssspExecutors[i]);
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new TaskBarrier(barriers));
        }

        System.out.println("[DEBUG] SSSP Driver Init Done");
    }

    public void run()
            throws BrokenBarrierException, InterruptedException {
        // TODO:
        graph.getPartition(0).update(0, 0.0);
        graph.getPartition(0).setBucketId(0, bucketIdx);
        graph.getPartition(0).setCurrMaxBucket(bucketIdx);
        graph.getPartition(0).setInnerIdx(innerIdx - 1);

        while (true) {
            if (totalDone()) {
                break;
            }
            for (int i = 0; i < graph.getNumPartitions(); i++) {
                graph.getPartition(i).setInnerIdx(0);
            }

            SSSPExecutor.setIsHeavy(false);
            while (true) {
                runLightEdges(workTasks);
                if (lightIsDone) {
                    break;
                }
                pushBarriers(barrierTasks);

                barriers.await();
                barriers.reset();
                innerIdx++;
            }
            System.out.println("[DEBUG] inner : " + innerIdx);

            SSSPExecutor.setIsHeavy(true);
            runHeavyEdges(workTasks);
            pushBarriers(barrierTasks);
            barriers.await();
            barriers.reset();

            bucketIdx++;
            innerIdx = 1;
            lightIsDone = false;
        }
    }

    public void print() {
        try (FileWriter fw = new FileWriter("sssp.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
            for (int i = 0; i < graph.getNumPartitions(); i++) {
                SSSPPartition partition = graph.getPartition(i);
                int offset = i << graph.getExpOfPartitionSize();
//            for (int j = 0; j < partition.getSize(); j++) {
//                int nodeId = offset + j;
//                String distance = String.format("%.3f", partition.getVertexValue(j));
//                System.out.println(nodeId + "   " + distance);
//            }

                for (int j = 0; j < partition.getSize(); j++) {
                    int nodeId = offset + j;
                    String distance = String.format("%.3f", partition.getVertexValue(j));
                    out.println(nodeId + " " + distance);
                }
            }
        }
        catch (IOException e) {

        }
    }

    public void runLightEdges(Task[] tasks) {
        int count = 0;
        for (int i = 0; i < tasks.length; i++) {
            if (!isLightEdgesDone(i)) {
                taskQueue.offer(tasks[i]);
                count++;
            }
        }

        if (count == 0) {
            lightIsDone = true;
        }

        lock.lock();
        try {
            condition.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public void runHeavyEdges(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.offer(tasks[i]);
        }
    }

    public void pushBarriers(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.offer(tasks[i]);
        }
    }

    public void reset() {
        for (int i = 0; i < graph.getNumPartitions(); i++) {
            graph.getPartition(i).reset();
        }
        lightIsDone = false;
        bucketIdx = 0;
        innerIdx = 1;
    }

    public boolean totalDone() {
        SSSPPartition[] partitions = graph.getPartitions();
        for (int i = 0; i < graph.getNumPartitions(); i++) {
            if (partitions[i].getCurrMaxBucket() >= bucketIdx) {
                return false;
            }
        }
        return true;
    }

    public boolean isLightEdgesDone(int partitionId) {
        SSSPPartition partition = graph.getPartition(partitionId);

        if (partition.getInnerIdx() == (innerIdx - 1)) {
            return false;
        }
        else {
            return true;
        }
    }

    public static TIntArrayList[] getLightEdges() {
        return lightEdges;
    }

    public static TIntArrayList[] getHeavyEdges() {
        return heavyEdges;
    }

    public static TDoubleArrayList[] getLightWeights() {
        return lightWeights;
    }

    public static TDoubleArrayList[] getHeavyWeights() {
        return heavyWeights;
    }

    public static int getInnerIdx() {
        return innerIdx;
    }

    public static int getBucketIdx() {
        return bucketIdx;
    }
}