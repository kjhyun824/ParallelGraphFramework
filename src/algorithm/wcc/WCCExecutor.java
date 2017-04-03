package algorithm.wcc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerPartition;

public class WCCExecutor implements GraphAlgorithmInterface{

    DirectedGraph<IntegerPartition> graph;
    IntegerPartition partition;

    WCCExecutor(DirectedGraph<IntegerPartition> graph) {
        this.graph = graph;
    }

    @Override
    public void execute(int partitionId) {
        partition = graph.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int expOfPartitionSize = graph.getExpOfPartitionSize();
        int offset = partitionId << expOfPartitionSize;

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;

            Node srcNode = graph.getNode(nodeId);

            if(srcNode != null) {
                update(srcNode,nodeId);
            }
        }
    }

    public void update(Node srcNode,int nodeId) {
        int neighborListSize = srcNode.neighborListSize();

        for (int j = 0; j < neighborListSize; j++) {
            int destId = srcNode.getNeighbor(j);
            int destPartitionId = graph.getPartitionId(destId);
            IntegerPartition destPartition = graph.getPartition(destPartitionId);
            int destPosition = graph.getNodePositionInPart(destId);

            destPartition.update(destPosition,nodeId);
        }
    }

    @Override
    public void reset(int taskId) {

    }
}