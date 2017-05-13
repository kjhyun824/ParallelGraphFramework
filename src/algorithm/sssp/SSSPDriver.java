package algorithm.sssp;

import gnu.trove.list.array.TIntArrayList;
import graph.Graph;
import graph.Node;
import graph.partition.SSSPPartition;
import task.Task;
import task.BarrierTask;
import thread.TaskWaitingRunnable;
import thread.ThreadUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SSSPDriver
{
    final int numThreads;
    final int delta;
    final int numCheck;
    final int sourceId;
    boolean lightIsDone;

    static int innerIdx;
    static int bucketIdx;

    static AtomicInteger before, after;
    static AtomicInteger killAfterFive;

    static TIntArrayList[] lightEdges;
    static TIntArrayList[] lightWeights;
    static TIntArrayList[] heavyEdges;
    static TIntArrayList[] heavyWeights;

    Graph<SSSPPartition> graph;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    Task[] workTasks;
    Task[] barrierTasks;

    SSSPExecutor[] ssspExecutors;

    public SSSPDriver(Graph<SSSPPartition> graph, int numThreads, int delta, int source, int numCheck) {
        this.graph = graph;
        this.numThreads = numThreads;
        this.delta = delta;
        this.sourceId = source;
        this.numCheck = numCheck;
        this.lightIsDone = false;
        this.bucketIdx = 0;
        this.innerIdx = 1;
//        before = new AtomicInteger(0);
//        after = new AtomicInteger(0);
//        killAfterFive = new AtomicInteger(0);

        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();

        workTasks = new Task[numPartitions];
        ssspExecutors = new SSSPExecutor[numPartitions];
        barrierTasks = new Task[numThreads];

        barriers = new CyclicBarrier(numThreads + 1);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

//        int numNodes = graph.getNumNodes();
        int maxNodeId = graph.getMaxNodeId();
        lightEdges = new TIntArrayList[maxNodeId + 1];
        lightWeights = new TIntArrayList[maxNodeId + 1];
        heavyEdges = new TIntArrayList[maxNodeId + 1];
        heavyWeights = new TIntArrayList[maxNodeId + 1];

        for (int i = 0; i <= maxNodeId; i++) {
            lightEdges[i] = new TIntArrayList();
            lightWeights[i] = new TIntArrayList();
            heavyEdges[i] = new TIntArrayList();
            heavyWeights[i] = new TIntArrayList();

            Node srcNode = graph.getNode(i);

            if (srcNode == null) {
                continue;
            }

            int exp_delta = 1 << delta;
            for (int j = 0; j < srcNode.getOutDegree(); j++) {
                int destId = srcNode.getNeighbor(j);
                int weight = srcNode.getWeight(destId);
                if (weight > exp_delta) {
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
            ssspExecutors[i] = new SSSPExecutor(i, graph, delta, numCheck);
            workTasks[i] = new Task(ssspExecutors[i]);
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new BarrierTask(barriers));
        }

        System.err.println("[DEBUG] SSSP Driver Init Done");
    }

    public void run() throws BrokenBarrierException, InterruptedException {
        // TODO:
        graph.getPartition(0).update(0, 0);
        graph.getPartition(0).setBucketId(0, bucketIdx);
        graph.getPartition(0).setCurrMaxBucket(bucketIdx);
        graph.getPartition(0).setInnerIdx(innerIdx - 1);

        int count = 0;
        while (true) {
            if (totalDone()) {
                System.out.println("[DEBUG] Total Iteration Count : " + count);
                break;
            }
            count++;

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
                innerIdx++;
            }
            //System.err.println("[DEBUG] inner : " + innerIdx);

            SSSPExecutor.setIsHeavy(true);
            runHeavyEdges(workTasks);
            pushBarriers(barrierTasks);
            barriers.await();

            bucketIdx++;
            innerIdx = 1;
            lightIsDone = false;
        }

//        System.out.println("[DEBUG] Killed After Five non-updates : " + killAfterFive);
//        System.out.println("[DEBUG] Before / After : " + before + " / " + after);
    }

    public void print() {
        String fileName = "SSSP_Thr" + numThreads + "_Delta" + delta + "_Check"+numCheck+".txt";
        try (FileWriter fw = new FileWriter(fileName, true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
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
                    int dist = partition.getVertexValue(j);
                    if(dist == Integer.MAX_VALUE) {
                       dist = -1;
                    }
                    out.println(nodeId + "," + dist);
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

//        before.set(0);
//        after.set(0);
//        killAfterFive.set(0);
    }

    public boolean totalDone() {
        SSSPPartition[] partitions = graph.getPartitions();
        for (int i = 0; i < graph.getNumPartitions(); i++) {
            if (partitions[i].getCurrMaxBucket() >= bucketIdx) {
                return false;
            }
        }

        for (int i = 0; i < graph.getNumPartitions(); i++) {
            int partitionSize = partitions[i].getSize();
            for (int j = 0; j < partitionSize; j++) {
                if (partitions[i].getBucketId(j) >= bucketIdx) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isLightEdgesDone(int partitionId) {
        SSSPPartition partition = graph.getPartition(partitionId);

        if (partition.getInnerIdx() >= (innerIdx - 1)) {
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

    public static TIntArrayList[] getLightWeights() {
        return lightWeights;
    }

    public static TIntArrayList[] getHeavyWeights() {
        return heavyWeights;
    }

    public static int getInnerIdx() {
        return innerIdx;
    }

    public static int getBucketIdx() {
        return bucketIdx;
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

    public static void incKillAfterFive() {
        int temp;
        do {
            temp = killAfterFive.get();
        } while (!killAfterFive.compareAndSet(temp,temp+1));
    }
}
