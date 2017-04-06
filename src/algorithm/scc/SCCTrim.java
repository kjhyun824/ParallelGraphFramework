package algorithm.scc;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerPartition;

public class SCCTrim implements GraphAlgorithmInterface {
    Graph<IntegerPartition> graph;
    final int partitionId;
    boolean[] isInActive;

    public SCCTrim(int partitionId, Graph<IntegerPartition> graph, boolean[] isInActive) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.isInActive = isInActive;
    }

    @Override
    public void execute() {
        int partitionSize = graph.getPartition(partitionId).getSize();
        int offset = partitionId * partitionSize;

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            Node node = graph.getNode(nodeId);
            if (node != null) {
                if ((node.getInDegree() == 0 && node.getOutDegree() == 1) || (node.getInDegree() == 1 && node.getOutDegree() == 0)) {
                    isInActive[nodeId] = true;
                }
            }
        }
    }

    @Override
    public void reset() {

    }
}

