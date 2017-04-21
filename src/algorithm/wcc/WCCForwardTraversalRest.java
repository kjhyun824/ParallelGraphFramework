package algorithm.wcc;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.WCCPartition;

public class WCCForwardTraversalRest implements GraphAlgorithmInterface
{
    static final byte ACTIVE = 1;
    static final byte IN_ACTIVE = 0;

    Graph<WCCPartition> graph;
    WCCPartition partition;
    final int partitionId;
    final int offset;
    final int partitionSize;

    public WCCForwardTraversalRest(int partitionId, Graph<WCCPartition> graph) {
        this.partitionId = partitionId;
        this.graph = graph;
        partition = graph.getPartition(partitionId);
        offset = partitionId << graph.getExpOfPartitionSize();
        partitionSize = partition.getSize();
    }

    @Override
    public void execute() {
        partition.setPartitionActiveValue(IN_ACTIVE);
        for (int i = 0; i < partitionSize; i++) {
            int srcId = offset + i;
            Node srcNode = graph.getNode(srcId);

            if (srcNode == null) {
                continue;
            }

            int curCompId = partition.getCurCompId(i);
            int nextCompId = partition.getNextCompId(i);

            if (curCompId == nextCompId) {
                continue;
            }

            partition.setCurComponentId(i, nextCompId);

            int neighborListSize = srcNode.neighborListSize();

            for (int j = 0; j < neighborListSize; j++) {
                int destId = srcNode.getNeighbor(j);
                int destPartitionId = graph.getPartitionId(destId);

                WCCPartition destPartition = graph.getPartition(destPartitionId);
                int destPosition = graph.getNodePositionInPart(destId);

                if (destPartition.update(destPosition, nextCompId)) {
                    destPartition.setPartitionActiveValue(ACTIVE);
                }
            }
        }
    }

    @Override
    public void reset() {

    }
}
