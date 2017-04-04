package algorithm.pagerank;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.DoublePartition;

public class PageRankExecutor implements GraphAlgorithmInterface{
    DirectedGraph<DoublePartition> graph;
    DoublePartition doublePartition;
    Node srcNode;
    double dampingFactor;

    PageRankExecutor(DirectedGraph<DoublePartition> graph, double dampingFactor) {
        this.graph = graph;
        this.dampingFactor = dampingFactor;
    }

    @Override
    public void execute(int partitionId) {
        doublePartition = graph.getPartition(partitionId);
        int partitionSize = doublePartition.getSize();
        int expOfPartitionSize = graph.getExpOfPartitionSize();
        int offset = partitionId << expOfPartitionSize;

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            srcNode = graph.getNode(nodeId);

            if (srcNode != null) {
                update(i);
            }
        }
    }

    public void update(int entry) {
        int neighborListSize = srcNode.neighborListSize();
        double scatteredPageRank = getScatteredPageRank(doublePartition, entry, neighborListSize);

        for (int j = 0; j < neighborListSize; j++) {
            int dest = srcNode.getNeighbor(j);
            int destPartitionId = graph.getPartitionId(dest);

            DoublePartition destDoublePartition = graph.getPartition(destPartitionId);
            int destPosition = graph.getNodePositionInPart(dest);

            destDoublePartition.updateNextTable(destPosition, scatteredPageRank);
        }
    }

    public double getScatteredPageRank(DoublePartition doublePartition, int index, int neighborListSize) {
        return dampingFactor * doublePartition.getVertexValue(index) / (double) neighborListSize;
    }

    @Override
    public void reset(int taskId) {

    }
}
