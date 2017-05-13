package algorithm.pagerank.personalized;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.PersonalPageRankPartition;

public class PersonalPageRankExecutor implements GraphAlgorithmInterface
{
    Graph<PersonalPageRankPartition> graph;
    PersonalPageRankPartition partition;
    Node srcNode;

    int partitionId;
    int partitionSize;
    int offset;

    double dampingFactor;

    PersonalPageRankExecutor(int partitionId, Graph<PersonalPageRankPartition> graph, double dampingFactor) {
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
            srcNode = graph.getNode(offset + i);

            if (srcNode == null) {
                continue;
            }

            int neighborListSize = srcNode.neighborListSize();
            double curPageRank = partition.getVertexValue(i);
            if (curPageRank == 0) {
                continue;
            }
            double scatterPageRank = dampingFactor * (curPageRank / (double) neighborListSize);


            for (int j = 0; j < neighborListSize; j++) {
                int destId = srcNode.getNeighbor(j);
                int destPartitionId = graph.getPartitionId(destId);

                PersonalPageRankPartition destPartition = graph.getPartition(destPartitionId);
                int destPosition = graph.getNodePositionInPart(destId);
                destPartition.updateNextTable(destPosition, scatterPageRank);
            }
        }
    }

/*
// Active Node Check version
    @Override
    public void execute() {
        for (int i = 0; i < partitionSize; i++) {
            srcNode = graph.getNode(offset + i);

            if (srcNode == null || !partition.isNodeSeed(i)) {
                continue;
            }

            int neighborListSize = srcNode.neighborListSize();
            double scatterPageRank = dampingFactor * (partition.getVertexValue(i) / (double) neighborListSize);

            for (int j = 0; j < neighborListSize; j++) {
                int dest = srcNode.getNeighbor(j);
                int destPartitionId = graph.getPartitionId(dest);

                PersonalPageRankPartition destPartition = graph.getPartition(destPartitionId);
                int destPosition = graph.getNodePositionInPart(dest);
                // need active Check
                destPartition.updateNextTable(destPosition, scatterPageRank);
            }
        }
    }
*/
    @Override
    public void reset() {

    }
}