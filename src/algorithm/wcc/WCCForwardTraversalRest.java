package algorithm.wcc;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerPartition;

public class WCCForwardTraversalRest implements GraphAlgorithmInterface {
    static final byte ACTIVE = 1;
    static final byte IN_ACTIVE = 0;

    Graph<IntegerPartition> graph;
    IntegerPartition partition;
    final int partitionId;
    final int offset;
    final int partitionSize;

    public WCCForwardTraversalRest(int partitionId, Graph<IntegerPartition> graph) {
        this.partitionId = partitionId;
        this.graph = graph;
        partition = graph.getPartition(partitionId);
        offset = partitionId << graph.getExpOfPartitionSize();
        partitionSize = partition.getSize();
    }

    @Override
    public void execute() {
        partition.setPartitionActiveValue(IN_ACTIVE);
        for (int i = 0; i < partitionSize; i++) {
            int srcId = offset + i;
            Node node = graph.getNode(srcId);

            if (node != null) {
                int srcColor = partition.getNodeActiveValue(i);
                int nextSrcColor = partition.getVertexValue(i);

                if (srcColor != nextSrcColor) {
                    partition.setNodeIsActive(i, nextSrcColor);

                    int neighborListSize = node.neighborListSize();

                    for (int j = 0; j < neighborListSize; j++) {
                        int destId = node.getNeighbor(j);
                        int destPartitionId = graph.getPartitionId(destId);

                        IntegerPartition destPartition = graph.getPartition(destPartitionId);
                        int destPosition = graph.getNodePositionInPart(destId);
                        int destColor = destPartition.getNodeActiveValue(destPosition);

                        if (destColor < nextSrcColor) {
                            destPartition.update(destPosition, nextSrcColor);
                            destPartition.setPartitionActiveValue(ACTIVE);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void reset() {

    }
}
