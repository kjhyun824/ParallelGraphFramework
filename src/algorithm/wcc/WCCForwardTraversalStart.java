package algorithm.wcc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerGraphPartition;
import graph.partition.IntegerNodePartition;

public class WCCForwardTraversalStart implements GraphAlgorithmInterface {
    static final byte ACTIVE = 1;

    DirectedGraph graph;
    IntegerGraphPartition graphPartition;
    IntegerNodePartition partition;

    public WCCForwardTraversalStart(DirectedGraph<IntegerGraphPartition> graph) {
        this.graph = graph;
        graphPartition = graph.getPartitionInstance();
    }

    @Override
    public void execute(int partitionId) {
        partition = graphPartition.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int expOfPartitionSize = graphPartition.getExpOfPartitionSize();
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
