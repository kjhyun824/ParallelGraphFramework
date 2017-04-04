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
    int offset;

    public WCCForwardTraversalRest(int partitionId, Graph<IntegerPartition> graph) {
        this.partitionId = partitionId;
        this.graph = graph;
        partition = graph.getPartition(partitionId);
        offset = partitionId << graph.getExpOfPartitionSize();
    }

    @Override
    public void execute() {
        int partitionSize = partition.getSize();

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            int srcColor = partition.getVertexValue(i);

            if (partition.checkNodeIsActive(i, ACTIVE)) {
                partition.setNodeIsActive(i, IN_ACTIVE);

                Node node = graph.getNode(nodeId);
                int neighborListSize = node.neighborListSize();

                for (int j = 0; j < neighborListSize; j++) {
                    int destId = node.getNeighbor(j);
                    int destPartitionId = graph.getPartitionId(destId);

                    IntegerPartition destPartition = graph.getPartition(destPartitionId);
                    int destPosition = graph.getNodePositionInPart(destId);
                    int destColor = destPartition.getVertexValue(destPosition);

                    if (destColor < srcColor) {
                        destPartition.update(destPosition, srcColor);
                        destPartition.setNodeIsActive(destPosition, ACTIVE);
                        destPartition.setPartitionActiveValue(ACTIVE);
                    }
                }
            }
        }
    }

    @Override
    public void reset() {

    }
}
