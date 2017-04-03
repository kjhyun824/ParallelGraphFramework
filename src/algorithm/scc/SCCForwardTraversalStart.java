package algorithm.scc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.partition.IntegerPartition;

public class SCCForwardTraversalStart implements GraphAlgorithmInterface {
    DirectedGraph<IntegerPartition> graph;

    public SCCForwardTraversalStart(DirectedGraph<IntegerPartition> graph) {
        this.graph = graph;
    }

    @Override
    public void execute(int partitionId) {
        int partitionSize = graph.getPartition(partitionId).getSize();
        int offset = partitionId * partitionSize;
        IntegerPartition partition = graph.getPartition(partitionId);       // partition 안에 table이 있는게 의미상 이상함

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            partition.setVertexValue(i, nodeId);
        }
    }

    @Override
    public void reset(int partitionId) {

    }
}
