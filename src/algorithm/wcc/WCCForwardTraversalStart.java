package algorithm.wcc;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerPartition;

public class WCCForwardTraversalStart implements GraphAlgorithmInterface {
    Graph<IntegerPartition> graph;
    IntegerPartition partition;
    final int partitionId;
    int offset;
    int partitionSize;

    public WCCForwardTraversalStart(int partitionId, Graph<IntegerPartition> graph) {
        this.partitionId = partitionId;
        this.graph = graph;
        partition = graph.getPartition(partitionId);
        offset = partitionId << graph.getExpOfPartitionSize();
        partitionSize = partition.getSize();
    }

    @Override
    public void execute() {
        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            Node node = graph.getNode(nodeId);

            if (node != null) {
                partition.setVertexValue(i, nodeId);
            }
        }
    }

    @Override
    public void reset( ) {

    }
}
