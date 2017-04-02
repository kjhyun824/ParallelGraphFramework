package algorithm.scc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;

public class BackwardTraversalStart implements GraphAlgorithmInterface{
    DirectedGraph graph;
    boolean[] isInActive;

    public BackwardTraversalStart (DirectedGraph graph, boolean[] isInActive) {
        this.graph = graph;
        this.isInActive = isInActive;
    }

    @Override
    public void execute(int partitionId) {
        
    }

    @Override
    public void reset(int partitionId) {

    }
}
