package algorithm.wcc;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.WCCPartition;

public class WCCExecutor implements GraphAlgorithmInterface
{
    static final byte ACTIVE = 1;
    static final byte IN_ACTIVE = 0;

    final Graph<WCCPartition> graph;
    final WCCPartition partition;
    final int partitionId;
    final int offset;
    final int partitionSize;

    public WCCExecutor(int partitionId, Graph<WCCPartition> graph) {
        this.partitionId = partitionId;
        this.graph = graph;
        partition = graph.getPartition(partitionId);
        offset = partitionId << graph.getExpOfPartitionSize();
        partitionSize = partition.getSize();
    }

    @Override
    public void execute() {
        partition.setPartitionActiveValue(IN_ACTIVE);
        outerLoop();
    }

    @Override
    public void reset() {

    }

    public void outerLoop() {
        for (int i = 0; i < partitionSize; i++) {
            int srcId = offset + i;
            Node srcNode = graph.getNode(srcId);

            if (srcNode == null) {
                continue;
            }

            int curCompId = partition.getCurCompId(i);
            int nextCompId = partition.getNextCompId(i);    //  2%

            if (curCompId == nextCompId) {
                continue;
            }

            partition.setCurComponentId(i, nextCompId);

            innerLoop(srcNode, nextCompId);
        }
    }

    public void innerLoop(Node srcNode, int nextCompId) {                     // 81%
        int neighborListSize = srcNode.neighborListSize();                    // 2%
        for (int j = 0; j < neighborListSize; j++) {
            int destId = srcNode.getNeighbor(j);                              // 4%

            if (destId <= nextCompId) {
                continue;
            }

            int destPartitionId = graph.getPartitionId(destId);               // 2%

            WCCPartition destPartition = graph.getPartition(destPartitionId); // 5%
            int destPosition = graph.getNodePositionInPart(destId);           // 14%

            if (destPartition.update(destPosition, nextCompId)) {             // 50%
                destPartition.setPartitionActiveValue(ACTIVE);
            }
        }
    }
}