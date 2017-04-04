package algorithm.wcc;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerPartition;

public class WCCForwardTraversalStart implements GraphAlgorithmInterface {
    static final byte ACTIVE = 1;

    Graph<IntegerPartition> graph;
    IntegerPartition partition;
    final int partitionId;
    int offset;

    public WCCForwardTraversalStart(int partitionId, Graph<IntegerPartition> graph) {
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
            Node node = graph.getNode(nodeId);

            if (node != null) {
                partition.setVertexValue(i, nodeId);
                partition.setNodeIsActive(i, ACTIVE);
            }
        }
    }

    @Override
    public void reset( ) {

    }
}
