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
    final int numCheck;
    static boolean isFront;

    public WCCExecutor(int partitionId, Graph<WCCPartition> graph, int numPart, int numCheck) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.numCheck = numCheck;
        partition = graph.getPartition(partitionId);
        offset = partitionId << graph.getExpOfPartitionSize();
        partitionSize = partition.getSize();

        isFront = true;
        threshold = numPart * partitionSize;
    }

    @Override
    public void execute() {
        int epoch = WCCDriver.getCurrentEpoch();
//        if (epoch == 1) System.out.println("[DEBUG] Threshold : " + threshold);

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

            boolean updateFlag = false;
            int check = 0;
            for (int j = 0; j < neighborListSize; j++) {
                int destId = srcNode.getNeighbor(j);                                            // 3% (256)     4% (4096)   4%(65536)
                if( destId <= nextCompId ) continue;

                WCCPartition destPart = graph.getPartition(graph.getPartitionId(destId));       // 7% (256)     8% (4096)   8%(65536)
                int destPos = graph.getNodePositionInPart(destId);                              // 14% (256)    6% (4096)   3%(65536)

//                WCCDriver.incBefore();
                if (destPart.update(destPos, nextCompId)) { //destPartition.update(destPosition, nextCompId)) {
                    updateFlag = true;
                    destPart.setUpdatedEpoch(epoch);
//                    WCCDriver.incAfter();
//                    destPartition.setUpdatedEpoch(epoch);
                } else {
                    if(numCheck != -1 && !updateFlag) {
                        check++;
                        if (check >= numCheck) {
                            break;
                        }
                    }
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
