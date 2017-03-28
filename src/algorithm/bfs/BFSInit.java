package algorithm.bfs;

/**
 * Created by JungHyun on 2017-03-27.
 */

import graph.DirectedGraph;
import graph.NodePartition;

public class BFSInit extends BFS {
    int iteration;

    BFSInit(DirectedGraph graph) {
        super(graph);
        iteration = 0;
    }

    @Override
    public void execute(int partitionId) {
        NodePartition partition = graphPartition.getPartition(partitionId);
        int partitionLength = partition.getPartitionSize();
        table = partition.getTable();

        for(int i = 0; i < partitionLength; i++) {
            int nodeId = graphPartition.getNodeNumberInPart(partitionId,i);
            srcNode = graph.getNode(nodeId);
        }
    }
}
