package algorithm.wcc;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.IntegerGraphPartition;
import graph.partition.IntegerNodePartition;

public class WCCExecutor implements GraphAlgorithmInterface{

    DirectedGraph<IntegerGraphPartition> graph;
    IntegerGraphPartition graphPartition;
    IntegerNodePartition partition;

    WCCExecutor(DirectedGraph<IntegerGraphPartition> graph) {
        this.graph = graph;

        graphPartition = graph.getPartitionInstance();
    }

    @Override
    public void execute(int partitionId) {
        partition = graphPartition.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int expOfPartitionSize = graphPartition.getExpOfPartitionSize();
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
            int destPartitionId = graphPartition.getPartitionId(destId);
            IntegerNodePartition destPartition = graphPartition.getPartition(destPartitionId);
            int destPosition = graphPartition.getNodePositionInPart(destId);

            destPartition.update(destPosition,nodeId);
        }
    }

    @Override
    public void reset(int taskId) {

    }
}