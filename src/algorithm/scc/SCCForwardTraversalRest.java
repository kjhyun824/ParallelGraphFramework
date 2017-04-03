package algorithm.scc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerPartition;

public class SCCForwardTraversalRest implements GraphAlgorithmInterface {
    DirectedGraph<IntegerPartition> graph;
    boolean[] isInActive;

    public SCCForwardTraversalRest(DirectedGraph<IntegerPartition> graph, boolean[] isInActive) {
        this.graph = graph;
        this.isInActive = isInActive;
    }

    @Override
    public void execute(int partitionId) {
        int partitionSize = graph.getPartition(partitionId).getSize();
        int offset = partitionId * partitionSize;
        IntegerPartition partition = graph.getPartition(partitionId);

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            int colorId = partition.getVertexValue(i);

            if (!isInActive[nodeId]) {
                Node node = graph.getNode(nodeId);

                if (node != null) {
                    int neighborListSize = node.neighborListSize();

                    for (int j = 0; j < neighborListSize; j++) {
                        int neighborId = node.getNeighbor(j);

                        if (!isInActive[neighborId]) {
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
