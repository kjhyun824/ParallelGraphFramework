package algorithm.bfs;

/**
 * Created by JungHyun on 2017-03-27.
 */

import graph.GraphPartition;
import graph.Node;
import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import table.UpdateTable;

public abstract class BFS implements GraphAlgorithmInterface {
    DirectedGraph graph;
    GraphPartition graphPartition;
    UpdateTable table;

    Node srcNode;

    BFS (DirectedGraph graph) {
       this.graph = graph;

       graphPartition = graph.getPartitionInstance();
    }
}
