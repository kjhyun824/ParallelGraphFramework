package algorithm.wcc;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.partition.WCCPartition;

public class WCCForwardTraversalStart implements GraphAlgorithmInterface
{
    Graph<WCCPartition> graph;
    WCCPartition partition;
    final int partitionId;
    final int offset;
    final int partitionSize;

    public WCCForwardTraversalStart(int partitionId, Graph<WCCPartition> graph) {
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

            if (graph.getNode(nodeId) != null) {
                partition.setNextCompId(i, nodeId);
            }
        }
    }

    @Override
    public void reset() {

    }
}
