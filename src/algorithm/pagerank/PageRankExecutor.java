package algorithm.pagerank;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.PageRankPartition;

public class PageRankExecutor implements GraphAlgorithmInterface
{
    Graph<PageRankPartition> graph;
    PageRankPartition partition;
    Node srcNode;

    int partitionId;
    int partitionSize;
    int offset;

    double dampingFactor;

    PageRankExecutor(int partitionId, Graph<PageRankPartition> graph, double dampingFactor) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.dampingFactor = dampingFactor;
        partition = graph.getPartition(partitionId);
        partitionSize = partition.getSize();
        offset = partitionId << graph.getExpOfPartitionSize();
    }

    @Override
    public void execute() {
        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            srcNode = graph.getNode(nodeId);

            update(i);
        }
    }

    public void update(int entry) {
        int neighborListSize = srcNode.neighborListSize();
        double scatteredPageRank = getScatteredPageRank(partition, entry, neighborListSize);

        for (int j = 0; j < neighborListSize; j++) {
            int dest = srcNode.getNeighbor(j);
            int destPartitionId = graph.getPartitionId(dest);

            PageRankPartition destPartition = graph.getPartition(destPartitionId);
            int destPosition = graph.getNodePositionInPart(dest);

            destPartition.updateNextTable(destPosition, scatteredPageRank);
        }
    }

    public double getScatteredPageRank(PageRankPartition partition, int index, int neighborListSize) {
        return dampingFactor * partition.getVertexValue(index) / (double) neighborListSize;
    }

    @Override
    public void reset() {

    }
}
