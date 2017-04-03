package algorithm.pagerank;

import graph.DirectedGraph;
import graph.partition.DoublePartition;

public class PageRankExecutor
        extends PageRank {

    PageRankExecutor(DirectedGraph<DoublePartition> graph, double dampingFactor) {
        super(graph, dampingFactor);
    }

    @Override
    public void execute(int partitionId) {
        doublePartition = graph.getPartition(partitionId);
        int partitionLength = doublePartition.getSize();

		for (int i = 0; i < partitionLength; i++) {
			int nodeId = graph.getNodeNumberInPart(partitionId, i);
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
            int destPartitionNumber = graph.getPartitionId(dest);

            DoublePartition destDoublePartition = graph.getPartition(destPartitionNumber);

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
