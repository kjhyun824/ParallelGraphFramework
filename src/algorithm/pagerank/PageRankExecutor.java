package algorithm.pagerank;

import graph.DirectedGraph;
import graph.NodePartition;

public class PageRankExecutor
        extends PageRank {

    PageRankExecutor(DirectedGraph graph, double dampingFactor) {
        super(graph, dampingFactor);
    }

    @Override
    public void execute(int partitionId) {
        partition = graphPartition.getPartition(partitionId);
        int partitionLength = partition.getSize();

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
        double scatteredPageRank = getScatteredPageRank(partition, entry, neighborListSize);

        for (int j = 0; j < neighborListSize; j++) {
            int dest = srcNode.getNeighbor(j);
            int destPartitionNumber = graphPartition.getPartitionNumber(dest);

            NodePartition destPartition = graphPartition.getPartition(destPartitionNumber);

            int destPosition = graphPartition.getNodePositionInPart(dest);

            destPartition.updateNextTable(destPosition, scatteredPageRank);
        }
    }

    public double getScatteredPageRank(NodePartition partition, int index, int neighborListSize) {
        return dampingFactor * partition.getVertexValue(index) / (double) neighborListSize;
    }

    @Override
    public void reset(int taskId) {

    }
}
