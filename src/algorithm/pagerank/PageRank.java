package algorithm.pagerank;

import graph.partition.DoublePartition;
import graph.Node;
import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;

public abstract class PageRank implements GraphAlgorithmInterface {
	DirectedGraph<DoublePartition> graph;
    DoublePartition doublePartition;

	Node srcNode;

	double dampingFactor;

	PageRank (DirectedGraph<DoublePartition> graph, double dampingFactor) {
		this.graph = graph;
		this.dampingFactor = dampingFactor;
	}
}
