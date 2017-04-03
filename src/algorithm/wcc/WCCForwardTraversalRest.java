package algorithm.wcc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerPartition;

public class WCCForwardTraversalRest implements GraphAlgorithmInterface {
    static final byte ACTIVE = 1;

    DirectedGraph<IntegerPartition> graph;
    IntegerPartition partition;

    public WCCForwardTraversalRest(DirectedGraph<IntegerPartition> graph) {
        this.graph = graph;
    }

    @Override
    public void execute(int partitionId) {
        partition = graph.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int expOfPartitionSize = graph.getExpOfPartitionSize();
        int offset = partitionId << expOfPartitionSize;

        IntegerPartition partition = graph.getPartition(partitionId);

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            int colorId = partition.getVertexValue(i);

            if (partition.checkNodeIsActive(nodeId, ACTIVE)) {
                Node node = graph.getNode(nodeId);

                if (node != null) {
                    int neighborListSize = node.neighborListSize();

                    for (int j = 0; j < neighborListSize; j++) {
                        int neighborId = node.getNeighbor(j);

                        if (partition.checkNodeIsActive(neighborId, ACTIVE)) {
                            int nodeIdPositionInPart = graph.getNodePositionInPart(neighborId);
                            partition.update(nodeIdPositionInPart, colorId);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void reset(int partitionId) {

    }
}
