package algorithm.pagerank.personalized;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.PersonalPageRankPartition;

public class PersonalPageRankInit implements GraphAlgorithmInterface
{
    Graph<PersonalPageRankPartition> graph;
    PersonalPageRankPartition partition;

    final int numSeeds;
    final int partitionId;
    final int partitionSize;
    final int offset;
    final double dampingFactor;

    double initialValue;
    double randomTeleportValue;
    boolean isFirst;

    PersonalPageRankInit(int partitionId, Graph<PersonalPageRankPartition> graph, double dampingFactor, int numSeeds) {
        this.numSeeds = numSeeds;
        this.partitionId = partitionId;
        this.graph = graph;
        this.dampingFactor = dampingFactor;

        partition = graph.getPartition(partitionId);
        partitionSize = partition.getSize();
        offset = partitionId << graph.getExpOfPartitionSize();
        initialValue = getInitPageRankValue(numSeeds, 0); //initial PageRank Value
        randomTeleportValue = getInitPageRankValue(numSeeds, dampingFactor);
        isFirst = true;
    }

    @Override
    public void execute() {
        if (!isFirst) {
            initialValue = randomTeleportValue;
        }
        else {
            initNextTable();
        }

        for (int i = 0; i < partitionSize; i++) {
            Node node = graph.getNode(offset + i);
            if (node == null) {
                continue;
            }

            if (partition.isNodeSeed(i)) {
                partition.setVertexValue(i, initialValue);
            } else {
                partition.setVertexValue(i, 0);
            }
        }

        if (!isFirst) {
            partition.initializedCallback();
        }
        isFirst = false;
    }

    public void initNextTable() {
        for (int i = 0; i < partitionSize; i++) {
            Node node = graph.getNode(offset + i);
            if (node == null || !partition.isNodeSeed(i)) {
                continue;
            }
            partition.setNextVertexValue(i, randomTeleportValue);
        }
    }

    public double getInitPageRankValue(int numNodes, double dampingFactor) {
        return (1 - dampingFactor) / (double) numNodes;
    }

    public void reset() {
        isFirst = true;
        initialValue = getInitPageRankValue(numSeeds, 0);
        if (partition != null) {
            partition.reset();
        }
    }
}
