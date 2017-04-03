package algorithm.pagerank;

import graph.partition.DoubleGraphPartition;
import graph.partition.DoubleNodePartition;
import graph.partition.GraphPartition;
import graph.Node;
import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;

public abstract class PageRank implements GraphAlgorithmInterface {
	DirectedGraph graph;
	DoubleGraphPartition graphPartition;
    DoubleNodePartition doubleNodePartition;

	Node srcNode;

	double dampingFactor;

	PageRank (DirectedGraph graph, double dampingFactor) {
		this.graph = graph;
		this.dampingFactor = dampingFactor;

		graphPartition = graph.getPartitionInstance();
	}
}
