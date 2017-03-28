package algorithm.pagerank;

import graph.GraphPartition;
import graph.Node;
import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.NodePartition;

public abstract class PageRank implements GraphAlgorithmInterface {
	DirectedGraph graph;
	GraphPartition graphPartition;
    NodePartition partition;

	Node srcNode;

	double dampingFactor;

	PageRank (DirectedGraph graph, double dampingFactor) {
		this.graph = graph;
		this.dampingFactor = dampingFactor;

		graphPartition = graph.getPartitionInstance();
	}
}
