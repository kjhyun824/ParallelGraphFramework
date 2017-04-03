package algorithm.pagerank;

import graph.DirectedGraph;
import graph.partition.DoublePartition;

public class PageRankInit extends PageRank {
    double initialValue;
    double stopSurfValue;
    boolean isFirst;

    PageRankInit(DirectedGraph<DoublePartition> graph, double dampingFactor) {
        super(graph, dampingFactor);
        initialValue = getInitPageRankValue(0); //initial PageRank Value
        stopSurfValue = getInitPageRankValue(dampingFactor);
        isFirst = true;
    }

    @Override
    public void execute(int partitionId) {
        if (!isFirst) {
            initialValue = stopSurfValue;
        }
        else {
            initNextTable(partitionId);
        }

        doublePartition = graph.getPartition(partitionId);
        int partitionSize = doublePartition.getSize();

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = graph.getNodeNumberInPart(partitionId, i);
            srcNode = graph.getNode(nodeId);

            if (srcNode != null) {
                doublePartition.setVertexValue(i, initialValue);
            }
        }

        if (!isFirst) {
            doublePartition.initializedCallback();
        }
        isFirst = false;
    }

    public void initNextTable(int partitionId) {
        DoublePartition doublePartition = graph.getPartition(partitionId);
        int partitionSize = doublePartition.getSize();

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = graph.getNodeNumberInPart(partitionId, i);
            srcNode = graph.getNode(nodeId);

            if (srcNode != null) {
                doublePartition.setNextVertexValue(i, stopSurfValue);
            }
        }
    }

    public double getInitPageRankValue(double dampingFactor) {
        return (1 - dampingFactor) / (double) graph.getNumNodes();
    }

    public void reset(int taskId) {
        isFirst = true;
        initialValue = getInitPageRankValue(0);
        if (doublePartition != null) {
            doublePartition.reset();
        }
    }
}
