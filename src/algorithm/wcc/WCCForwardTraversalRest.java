package algorithm.wcc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerGraphPartition;
import graph.partition.IntegerNodePartition;

public class WCCForwardTraversalRest implements GraphAlgorithmInterface {
    static final byte ACTIVE = 1;

    DirectedGraph graph;
    IntegerGraphPartition graphPartition;
    IntegerNodePartition partition;

    public WCCForwardTraversalRest(DirectedGraph<IntegerGraphPartition> graph) {
        this.graph = graph;
        graphPartition = graph.getPartitionInstance();
    }

    @Override
    public void execute(int partitionId) {
        partition = graphPartition.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int expOfPartitionSize = graphPartition.getExpOfPartitionSize();
        int offset = partitionId << expOfPartitionSize;

        IntegerNodePartition partition = graphPartition.getPartition(partitionId);

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            int colorId = partition.getVertexValue(i);

            if (partition.checkIsActive(nodeId, ACTIVE)) {
                Node node = graph.getNode(nodeId);

                if (node != null) {
                    int neighborListSize = node.neighborListSize();

                    for (int j = 0; j < neighborListSize; j++) {
                        int neighborId = node.getNeighbor(j);

                        if (partition.checkIsActive(neighborId, ACTIVE)) {
                            int nodeIdPositionInPart = graphPartition.getNodePositionInPart(neighborId);
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
