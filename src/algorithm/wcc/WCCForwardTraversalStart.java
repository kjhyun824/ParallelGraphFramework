package algorithm.wcc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerPartition;

public class WCCForwardTraversalStart implements GraphAlgorithmInterface {
    static final byte ACTIVE = 1;

    DirectedGraph<IntegerPartition> graph;
    IntegerPartition partition;

    public WCCForwardTraversalStart(DirectedGraph<IntegerPartition> graph) {
        this.graph = graph;
    }

    @Override
    public void execute(int partitionId) {
        partition = graph.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int expOfPartitionSize = graph.getExpOfPartitionSize();
        int offset = partitionId << expOfPartitionSize;

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            Node node = graph.getNode(nodeId);

            if (node != null) {
                partition.setVertexValue(i, nodeId);
                partition.setIsActive(i, ACTIVE);
            }
        }
    }

    @Override
    public void reset(int partitionId) {

    }
}
