package algorithm.diam;

import gnu.trove.list.array.TIntArrayList;
import graph.Graph;
import graph.Node;
import graph.partition.DIAMPartition;
import task.BarrierTask;
import task.Task;
import thread.TaskWaitingRunnable;
import thread.ThreadUtil;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by junghyun on 2017. 5. 8..
 */
public class DIAMDriver {
    final int numThreads;
    final int numCheck;
    final int delta;
    int diameter;
    boolean lightIsDone;

    static int innerIdx;
    static int bucketIdx;

    static TIntArrayList[] lightEdges;
    static TIntArrayList[] lightWeights;
    static TIntArrayList[] heavyEdges;
    static TIntArrayList[] heavyWeights;

    Graph<DIAMPartition> graph;
    LinkedBlockingQueue<Task> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    Task[] workTasks;
    Task[] barrierTasks;

    DIAMExecutor[] diamExecutors;

    public DIAMDriver(Graph<DIAMPartition> graph, int numThreads, int delta, int numCheck) {
        this.graph = graph;
        this.numThreads = numThreads;
        this.delta = delta;
        this.numCheck = numCheck;
        this.lightIsDone = false;
        this.bucketIdx = 0;
        this.innerIdx = 1;

        init();
    }

    public void init() {
        int numPartitions = graph.getNumPartitions();
        diameter = 0;

        workTasks = new Task[numPartitions];
        diamExecutors = new DIAMExecutor[numPartitions];
        barrierTasks = new Task[numThreads];

        barriers = new CyclicBarrier(numThreads + 1);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

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

            for (int j = 0; j < srcNode.getOutDegree(); j++) {
                int destId = srcNode.getNeighbor(j);
                int weight = srcNode.getWeight(destId);
                if (weight > delta) {
                    heavyEdges[i].add(destId);
                    heavyWeights[i].add(weight);
                } else {
                    lightEdges[i].add(destId);
                    lightWeights[i].add(weight);
                }
            }
        }

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            diamExecutors[i] = new DIAMExecutor(i, graph, delta, numCheck);
            workTasks[i] = new Task(diamExecutors[i]);
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new Task(new BarrierTask(barriers));
        }

        System.out.println("[DEBUG] Diameter Driver Init Done");
    }

    public void run() throws BrokenBarrierException, InterruptedException {
        /*
            1. Choose random Vertex
            2. Run sssp and Find maximum value
            3. If it's bigger than current diameter, update diameter and run again from the maximum vertex
         */
        Random random = new Random();
        int startId = random.nextInt() % graph.getNumNodes();
        diameter = 0;

        // TODO: startId Set
        while (true) {
            int startPartId = graph.getPartitionId(startId);
            DIAMPartition startPart = graph.getPartition(startPartId);
            int startPartPos = graph.getNodePositionInPart(startId);

            startPart.update(startPartPos, 0);
            startPart.setBucketId(startPartPos, bucketIdx);
            startPart.setCurrMaxBucket(bucketIdx);
            startPart.setInnerIdx(innerIdx - 1);

            while (true) { // Run SSSP
                if (totalDone()) {
                    break;
                }
                for (int i = 0; i < graph.getNumPartitions(); i++) {
                    graph.getPartition(i).setInnerIdx(0);
                }

                DIAMExecutor.setIsHeavy(false);
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

                DIAMExecutor.setIsHeavy(true);
                runHeavyEdges(workTasks);
                pushBarriers(barrierTasks);
                barriers.await();

                bucketIdx++;
                innerIdx = 1;
                lightIsDone = false;
            }
        /*
            TODO: Check the Longest SSSP is bigger than Current diameter. -> If true, set diameter and reset the shortest path table & run again from the maximum vertex.
         */
            int maxId = getMaxDistId();
            int maxDist = graph.getPartition(graph.getPartitionId(maxId)).getVertexValue(graph.getNodePositionInPart(maxId));
            if (maxDist > diameter) {
                diameter = maxDist;
                startId = maxId;
                reset();
            } else {
                break;
            }
        }
    }

    public int getMaxDistId() {
        int max = 0;
        int maxId = 0;
        for (int i = 0; i < graph.getNumPartitions(); i++) {
            DIAMPartition partition = graph.getPartition(i);
            for (int j = 0; j < partition.getSize(); j++) {
                int value = partition.getVertexValue(j);
                if (value > max) {
                    max = value;
                    maxId = (i * (1 << graph.getExpOfPartitionSize())) + j;
                }
            }
        }

        return maxId;
    }

    public void printDiameter() {
        System.out.println("[DEBUG] Diameter : " + diameter);
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
        DIAMPartition[] partitions = graph.getPartitions();
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
        DIAMPartition partition = graph.getPartition(partitionId);

        if (partition.getInnerIdx() >= (innerIdx - 1)) {
            return false;
        } else {
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
