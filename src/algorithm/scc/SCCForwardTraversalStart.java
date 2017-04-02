package algorithm.scc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.GraphPartition;
import graph.NodePartition;

public class SCCForwardTraversalStart implements GraphAlgorithmInterface {
    DirectedGraph graph;
    GraphPartition graphPartition;

    public SCCForwardTraversalStart(DirectedGraph graph) {
        this.graph = graph;
        graphPartition = graph.getPartitionInstance();
    }

    @Override
    public void execute(int partitionId) {
        int partitionSize = graphPartition.getPartition(partitionId).getSize();
        int offset = partitionId * partitionSize;
        NodePartition partition = graphPartition.getPartition(partitionId);       // partition 안에 table이 있는게 의미상 이상함

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            partition.setVertexValue(i, nodeId);
        }
    }

    @Override
    public void reset(int partitionId) {

    }
}
