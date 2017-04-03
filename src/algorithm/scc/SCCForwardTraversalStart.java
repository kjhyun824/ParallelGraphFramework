package algorithm.scc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.partition.IntegerGraphPartition;
import graph.partition.IntegerNodePartition;

public class SCCForwardTraversalStart implements GraphAlgorithmInterface {
    DirectedGraph graph;
    IntegerGraphPartition graphPartition;

    public SCCForwardTraversalStart(DirectedGraph<IntegerGraphPartition> graph) {
        this.graph = graph;
        graphPartition = graph.getPartitionInstance();
    }

    @Override
    public void execute(int partitionId) {
        int partitionSize = graphPartition.getPartition(partitionId).getSize();
        int offset = partitionId * partitionSize;
        IntegerNodePartition partition = graphPartition.getPartition(partitionId);       // partition 안에 table이 있는게 의미상 이상함

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            partition.setVertexValue(i, nodeId);
        }
    }

    @Override
    public void reset(int partitionId) {

    }
}
