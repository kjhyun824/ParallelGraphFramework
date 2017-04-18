package algorithm.pagerank;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.PageRankPartition;

public class PageRankExecutor implements GraphAlgorithmInterface{
    Graph<PageRankPartition> graph;
    PageRankPartition doublePartition;
    Node srcNode;

    final int partitionId;
    double dampingFactor;

    PageRankExecutor(int partitionId, Graph<PageRankPartition> graph, double dampingFactor) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.dampingFactor = dampingFactor;
    }

    @Override
    public void execute() {
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

            PageRankPartition destDoublePartition = graph.getPartition(destPartitionId);
            int destPosition = graph.getNodePositionInPart(dest);

            destDoublePartition.updateNextTable(destPosition, scatteredPageRank);
        }
    }

    public double getScatteredPageRank(PageRankPartition doublePartition, int index, int neighborListSize) {
        return dampingFactor * doublePartition.getVertexValue(index) / (double) neighborListSize;
    }

    @Override
    public void reset() {

    }
}
