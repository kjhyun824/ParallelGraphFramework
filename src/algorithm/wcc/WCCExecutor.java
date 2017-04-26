package algorithm.wcc;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.WCCPartition;

public class WCCExecutor implements GraphAlgorithmInterface {
    final Graph<WCCPartition> graph;
    final WCCPartition partition;
    final int partitionId;
    final int offset;
    final int partitionSize;
    final int threshold;
    static boolean isFront;

    public WCCExecutor(int partitionId, Graph<WCCPartition> graph, int numPart) {
        this.partitionId = partitionId;
        this.graph = graph;
        partition = graph.getPartition(partitionId);
        offset = partitionId << graph.getExpOfPartitionSize();
        partitionSize = partition.getSize();

        isFront = true;
        threshold = numPart * partitionSize;
    }

    @Override
    public void execute() {
        int epoch = WCCDriver.getCurrentEpoch();
        if (epoch == 1) System.out.println("[DEBUG] Threshold : " + threshold);

        for (int i = 0; i < partitionSize; i++) {
            int srcId = offset + i;
            Node srcNode = graph.getNode(srcId);

            if (srcNode == null) {
                continue;
            }

            int curCompId = partition.getCurCompId(i);
            int nextCompId = partition.getNextCompId(i);

            if (isFront) {
                if(nextCompId != 0) continue;
//                if (nextCompId > threshold) continue;
            } else {
                if(nextCompId == 0) continue;
//                if (nextCompId <= threshold) continue;
            }

            if (curCompId == nextCompId) {
                continue;
            }

            partition.setCurComponentId(i, nextCompId);

            int neighborListSize = srcNode.neighborListSize();

            for (int j = 0; j < neighborListSize; j++) {
//                WCCDriver.incBefore();
                int destId = srcNode.getNeighbor(j);
                if( destId <= nextCompId ) continue;

                WCCPartition destPart = graph.getPartition(graph.getPartitionId(destId));
                int destPos = graph.getNodePositionInPart(destId);
                /*
                int destNext = destPart.getNextCompId(destPos);

                if (destNext <= nextCompId) {
                    continue;
                }
                */

                /*
                int destPartitionId = graph.getPartitionId(destId);

                WCCPartition destPartition = graph.getPartition(destPartitionId);
                int destPosition = graph.getNodePositionInPart(destId);
                */

                if (destPart.update(destPos, nextCompId)) { //destPartition.update(destPosition, nextCompId)) {
                    destPart.setUpdatedEpoch(epoch);
//                    WCCDriver.incAfter();
//                    destPartition.setUpdatedEpoch(epoch);
                }
            }
        }
    }

    @Override
    public void reset() {

    }

    public static void setIsFront(boolean value) {
        isFront = value;
    }
}
