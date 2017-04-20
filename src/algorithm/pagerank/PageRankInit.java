package algorithm.pagerank;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.PageRankPartition;

public class PageRankInit implements GraphAlgorithmInterface
{
    Graph<PageRankPartition> graph;
    PageRankPartition partition;

    int partitionId;
    int partitionSize;
    int offset;
    double dampingFactor;
    double initialValue;
    double nextValue;
    boolean isFirst;

    PageRankInit(int partitionId, Graph<PageRankPartition> graph, double dampingFactor) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.dampingFactor = dampingFactor;

        partition = graph.getPartition(partitionId);
        partitionSize = partition.getSize();
        offset = partitionId << graph.getExpOfPartitionSize();
        initialValue = getInitPageRankValue(0); //initial PageRank Value
        nextValue = getInitPageRankValue(dampingFactor);
        isFirst = true;
    }

    @Override
    public void execute() {
        if (!isFirst) {
            initialValue = nextValue;
        }
        else {
            initNextTable();
        }

        for (int i = 0; i < partitionSize; i++) {
            Node node = graph.getNode(offset + i);
            if (node == null) {
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
            partition.setNextVertexValue(i, nextValue);
        }
    }

    public double getInitPageRankValue(double dampingFactor) {
        return (1 - dampingFactor) / (double) graph.getNumNodes();
    }

    public void reset() {
        isFirst = true;
        initialValue = getInitPageRankValue(0);
        if (partition != null) {
            partition.reset();
        }
    }
}
