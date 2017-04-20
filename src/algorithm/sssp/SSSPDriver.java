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

public class SSSPDriver
{
    int numThreads;
    int delta;
    boolean lightIsDone;

    static int innerIdx;
    static int bucketIdx;

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

    public SSSPDriver(Graph<SSSPPartition> graph, int numThreads, int delta, int source) {
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
            ssspExecutors[i] = new SSSPExecutor(i, graph, delta);
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
    }

    public void print() {
        try (FileWriter fw = new FileWriter("SSSP.txt", true); BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {
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
                    out.println(nodeId + "," + partition.getVertexValue(j));
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
}