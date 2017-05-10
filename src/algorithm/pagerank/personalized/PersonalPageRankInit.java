package algorithm.pagerank.personalized;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.PersonalPageRankPartition;

public class PersonalPageRankInit implements GraphAlgorithmInterface
{
    Graph<PersonalPageRankPartition> graph;
    PersonalPageRankPartition partition;
    static int activeNumNodes;

    int numSeeds;
    int partitionId;
    int partitionSize;
    int offset;
    double dampingFactor;
    double initialValue;
    double nextValue;
    boolean isFirst;

    public static void setActiveNumNodes (int value) {
        activeNumNodes = value;
    }

    PersonalPageRankInit(int partitionId, Graph<PersonalPageRankPartition> graph, double dampingFactor, int numSeeds) {
        this.numSeeds = numSeeds;
        this.partitionId = partitionId;
        this.graph = graph;
        this.dampingFactor = dampingFactor;

        partition = graph.getPartition(partitionId);
        partitionSize = partition.getSize();
        offset = partitionId << graph.getExpOfPartitionSize();
        initialValue = getInitPageRankValue(numSeeds, 0); //initial PageRank Value
        isFirst = true;
    }

    @Override
    public void execute() {
        if (!isFirst) {
            nextValue = getInitPageRankValue(activeNumNodes, dampingFactor);
            initialValue = nextValue;
        }
        else {
            initNextTable();
        }

        for (int i = 0; i < partitionSize; i++) {
            Node node = graph.getNode(offset + i);
            if (node == null || !partition.isNodeActive(i)) {
                continue;
            }
            partition.setVertexValue(i, initialValue);
        }

        if (!isFirst) {
            partition.initializedCallback();
        }
        isFirst = false;
    }

    public void initNextTable() {
        for (int i = 0; i < partitionSize; i++) {
            Node node = graph.getNode(offset + i);
            if (node == null || !partition.isNodeActive(i)) {
                continue;
            }
            partition.setNextVertexValue(i, nextValue);
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
