package algorithm.scc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;

public class BackwardTraversalStart implements GraphAlgorithmInterface{
    DirectedGraph graph;
    final int partitionId;
    boolean[] isInActive;

    public BackwardTraversalStart (int partitionId, DirectedGraph graph, boolean[] isInActive) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.isInActive = isInActive;
    }

    @Override
    public void execute() {
        
    }

    @Override
    public void reset() {

    }
}
