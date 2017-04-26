package algorithm.wcc;

import function.PredicateFunction;
import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.WCCPartition;

public class WCCExecutor implements GraphAlgorithmInterface
{
    final Graph<WCCPartition> graph;
    final WCCPartition partition;
    final int partitionId;
    final int offset;
    final int partitionSize;
    static int threshold;
    static PredicateFunction predicate;


    public static void setThreshold(int value) {
        threshold = value;
    }

    public static void setPredicateFunction(PredicateFunction predicateFunction) {
        predicate = predicateFunction;
    }

    public WCCExecutor(int partitionId, Graph<WCCPartition> graph) {
        this.partitionId = partitionId;
        this.graph = graph;
        partition = graph.getPartition(partitionId);
        offset = partitionId << graph.getExpOfPartitionSize();
        partitionSize = partition.getSize();
    }

    @Override
    public void execute() {
        int epoch = WCCDriver.getCurrentEpoch();

        for (int i = 0; i < partitionSize; i++) {
            int srcId = offset + i;
            Node srcNode = graph.getNode(srcId);

            if (srcNode == null) {
                continue;
            }

            int curCompId = partition.getCurCompId(i);
            int nextCompId = partition.getNextCompId(i);

            if (predicate.test(nextCompId, threshold)) {
                continue;
            }

            if (curCompId == nextCompId) {
                continue;
            }

            partition.setCurComponentId(i, nextCompId);

            int neighborListSize = srcNode.neighborListSize();

            for (int j = 0; j < neighborListSize; j++) {
                int destId = srcNode.getNeighbor(j);

                if (destId <= nextCompId) {
                    continue;
                }

                int destPartitionId = graph.getPartitionId(destId);
                WCCPartition destPartition = graph.getPartition(destPartitionId);
                int destPosition = graph.getNodePositionInPart(destId);

                partition.incrementTryUpdated();
                if (destPartition.update(destPosition, nextCompId)) {
                    destPartition.setUpdatedEpoch(epoch);
                    partition.incrementActualUpdated();
                }
            }
        }
    }

    @Override
    public void reset() {
    }
}
