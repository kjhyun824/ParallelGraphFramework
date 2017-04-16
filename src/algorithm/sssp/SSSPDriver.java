package algorithm.sssp;

import gnu.trove.list.array.TIntArrayList;
import graph.Graph;
import graph.Node;
import graph.partition.SSSPPartition;
import task.Task;
import task.TaskBarrier;
import thread.TaskWaitingRunnable;
import thread.ThreadUtil;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleBinaryOperator;

public class SSSPDriver {
    int numThreads;
    double delta;
    boolean lightIsDone;

    static int innerIdx;
    static int bucketIdx;

    static TIntArrayList[] lightEdges;
    static TIntArrayList[] heavyEdges;

    Graph<SSSPPartition> graph;
    DoubleBinaryOperator updateFunction;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    Task[] workTasks;
    Task[] barrierTasks;

    SSSPExecutor[] ssspExecutors;

    public SSSPDriver(Graph<SSSPPartition> graph, int numThreads, double delta) {
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

        updateFunction = getUpdateFunction();
        SSSPPartition.setUpdateFunction(updateFunction);

        workTasks = new Task[numPartitions];
        ssspExecutors = new SSSPExecutor[numPartitions];
        barrierTasks = new Task[numThreads];
        barriers = new CyclicBarrier(numThreads);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        int numNodes = graph.getNumNodes();
        lightEdges = new TIntArrayList[numNodes];
        heavyEdges = new TIntArrayList[numNodes];


        for (int i = 0; i < numNodes; i++) {
            lightEdges[i] = new TIntArrayList();
            heavyEdges[i] = new TIntArrayList();
            Node srcNode = graph.getNode(i);

            for (int j = 0; j < srcNode.getOutDegree(); j++) {
                int destId = srcNode.getNeighbor(j);
                if (srcNode.getNeighborWeight(destId) > delta) {
                    heavyEdges[i].add(destId);
                } else {
                    lightEdges[i].add(destId);
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

    public void run() {
        graph.getPartition(0).setBucketIds(0, bucketIdx);
        graph.getPartition(0).setCurrMaxBucket(bucketIdx);
        graph.getPartition(0).setInnerIter(innerIdx-1);

        while (true) {
            System.out.println("[DEBUG] Here");
            if (totalDone()) {
                break;
            }

            SSSPExecutor.setIsHeavy(false);
            while (true) {
                System.out.println("[DEBUG] There");
                runLightEdges(workTasks);
                if (lightIsDone) {
                    break;
                }
                runBarrierOnce(barrierTasks);
                busyWaitForSyncStopMilli(10);
                innerIdx++;
            }

            SSSPExecutor.setIsHeavy(true);
            runHeavyEdges(workTasks);
            runBarrierOnce(barrierTasks);
            busyWaitForSyncStopMilli(10);
            bucketIdx++;
            SSSPExecutor.setCurBucketId(bucketIdx);
            innerIdx = 1;
            lightIsDone = false;
        }
    }

    public void runLightEdges(Task[] tasks) {
        int count = 0;
        for (int i = 0; i < tasks.length; i++) {
            if (!checkDone(i, false)) {
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
            if (!checkDone(i, true)) {
                taskQueue.offer(tasks[i]);
            }
        }
    }

    public void runBarrierOnce(Task[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.offer(tasks[i]);
        }
    }

    public void busyWaitForSyncStopMilli(int millisecond) {
        while (taskQueue.size() != 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(millisecond);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public DoubleBinaryOperator getUpdateFunction() {
        DoubleBinaryOperator updateFunction = (prev, value) -> value;
        return updateFunction;
    }

    public void reset() {
        for (int i = 0; i < graph.getNumPartitions(); i++) {
            if (graph.getPartition(i) == null) {
                System.out.println("NULL");
            }
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

    public boolean checkDone(int partitionId, boolean isHeavy) {
        SSSPPartition partition = graph.getPartition(partitionId);
        if (!isHeavy) {
            if (partition.getInnerIter() == (innerIdx-1)) {
                return false;
            } else {
                return true;
            }
        } else {
            int offset = partitionId << graph.getExpOfPartitionSize();
            for (int i = 0; i < partition.getSize(); i++) {
                int nodeId = offset + i;
                if (partition.getBucketIds(i) == bucketIdx && !heavyEdges[nodeId].isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

    public static TIntArrayList[] getLightEdges() {
        return lightEdges;
    }

    public static TIntArrayList[] getHeavyEdges() {
        return heavyEdges;
    }

    public static int getInnerIdx() {
        return innerIdx;
    }

    public static int getBucketIdx() {
        return bucketIdx;
    }
}