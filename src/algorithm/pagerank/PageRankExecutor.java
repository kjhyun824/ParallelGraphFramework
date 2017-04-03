package algorithm.pagerank;

import graph.DirectedGraph;
import graph.partition.DoubleNodePartition;

public class PageRankExecutor
        extends PageRank {

    PageRankExecutor(DirectedGraph graph, double dampingFactor) {
        super(graph, dampingFactor);
    }

    @Override
    public void execute(int partitionId) {
        doubleNodePartition = (DoubleNodePartition) graphPartition.getPartition(partitionId);
        int partitionLength = doubleNodePartition.getSize();

		for (int i = 0; i < partitionLength; i++) {
			int nodeId = graphPartition.getNodeNumberInPart(partitionId, i);
			srcNode = graph.getNode(nodeId);

			if (srcNode != null) {
				update(i);
			}
		}
    }

    public void update(int entry) {
        int neighborListSize = srcNode.neighborListSize();
        double scatteredPageRank = getScatteredPageRank(doubleNodePartition, entry, neighborListSize);

        for (int j = 0; j < neighborListSize; j++) {
            int dest = srcNode.getNeighbor(j);
            int destPartitionNumber = graphPartition.getPartitionId(dest);

            DoubleNodePartition destDoubleNodePartition = (DoubleNodePartition) graphPartition.getPartition(destPartitionNumber);

            int destPosition = graphPartition.getNodePositionInPart(dest);

            destDoubleNodePartition.updateNextTable(destPosition, scatteredPageRank);
        }
    }

    public double getScatteredPageRank(DoubleNodePartition doubleNodePartition, int index, int neighborListSize) {
        return dampingFactor * doubleNodePartition.getVertexValue(index) / (double) neighborListSize;
    }

    @Override
    public void reset(int taskId) {

    }
}
