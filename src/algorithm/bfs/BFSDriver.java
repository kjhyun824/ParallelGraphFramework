package algorithm.bfs;

import graph.DirectedGraph;
import graph.GraphPartition;
import graph.NodePartition;
import task.*;
import thread.*;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleBinaryOperator;

public class BFSDriver {
    int level;
    int numThreads;

    DirectedGraph graph;
    GraphPartition graphPartition;
    DoubleBinaryOperator updateFunction;
    LinkedBlockingQueue<DoubleTask> taskQueue;
    TaskWaitingRunnable runnable;
    CyclicBarrier barriers;

    DoubleTask[] workTasks;
    DoubleTask[] barrierTasks;

    public BFSDriver(DirectedGraph graph, int numThreads) {
        this.graph = graph;
        this.numThreads = numThreads;
        this.level = 0;

        graphPartition = graph.getPartitionInstance();
        init();
    }

    public void init() {
        After3level();
        GraphPartition graphPartition = graph.getPartitionInstance();
        int numPartitions = graphPartition.getNumPartitions();

        updateFunction = getUpdateFunction();
        NodePartition.setUpdateFunction(updateFunction);

        workTasks = new DoubleTask[numPartitions];
        barrierTasks = new DoubleTask[numThreads];
        barriers = new CyclicBarrier(numThreads);

        taskQueue = new LinkedBlockingQueue<>();
        runnable = new TaskWaitingRunnable(taskQueue);

        ThreadUtil.createAndStartThreads(numThreads, runnable);

        for (int i = 0; i < numPartitions; i++) {
            workTasks[i] = new DoubleTask(i, new BFSExecutor(graph));
        }

        for (int i = 0; i < numThreads; i++) {
            barrierTasks[i] = new DoubleTask(i, new TaskBarrier(i, barriers));
        }
    }

    public void run() {
        BFSExecutor.setLevel(1);
        graphPartition.getPartition(0).setVertexValue(0, 1);
        for (int i = 0; i < 20; i++) {
            runOnce(workTasks);
            runOnce(barrierTasks);
        }
        busyWaitForSyncStopMilli(10);
    }

    public void runOnce(DoubleTask[] tasks) {
        for (int i = 0; i < tasks.length; i++) {
            taskQueue.add(tasks[i]);
        }
    }

    public void busyWaitForSyncStopMilli(int millisecond) {
        while (taskQueue.size() != 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(millisecond);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public DoubleBinaryOperator getUpdateFunction() {
        DoubleBinaryOperator updateFunction = (prev, value) -> value;
        return updateFunction;
    }

    public void After3level() {
        //TODO: Add 3 iterations before making parallel process
    }

    public void reset() {
        this.level = 0;
    }
}
