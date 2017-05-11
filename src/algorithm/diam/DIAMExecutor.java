package algorithm.diam;

import gnu.trove.list.array.TIntArrayList;
import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.DIAMPartition;

/**
 * Created by junghyun on 2017. 5. 8..
 */
public class DIAMExecutor implements GraphAlgorithmInterface {

    final Graph<DIAMPartition> graph;
    DIAMPartition partition;
    final int offset;
    final int delta;
    final int numCheck;
    static volatile boolean isHeavy;

    TIntArrayList[] lightEdges;
    TIntArrayList[] lightWeights;
    TIntArrayList[] heavyEdges;
    TIntArrayList[] heavyWeights;
    TIntArrayList edges = null;
    TIntArrayList weights = null;

    final int partitionId;

    DIAMExecutor(int partitionId, Graph<DIAMPartition> graph, int delta, int numCheck) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.delta = delta;
        this.numCheck = numCheck;
        this.lightEdges = DIAMDriver.getLightEdges();
        this.heavyEdges = DIAMDriver.getHeavyEdges();
        this.lightWeights = DIAMDriver.getLightWeights();
        this.heavyWeights = DIAMDriver.getHeavyWeights();
        this.offset = this.partitionId << graph.getExpOfPartitionSize();
    }
    @Override
    public void execute() {
        partition = graph.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int bucketIdx = DIAMDriver.getBucketIdx();

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            int currBucket = partition.getBucketId(i);
            if (currBucket == bucketIdx) {
                Node srcNode = graph.getNode(nodeId);

                if (srcNode == null) {
                    continue;
                }

                update(i, bucketIdx);
            }
        }
    }

    public void update(int srcNodeIdInPart, int bucketIdx) {
        if (!isHeavy) {
            edges = lightEdges[offset + srcNodeIdInPart];
            weights = lightWeights[offset + srcNodeIdInPart];
        }
        else {
            edges = heavyEdges[offset + srcNodeIdInPart];
            weights = heavyWeights[offset + srcNodeIdInPart];
        }

        int neighborListSize = edges.size();
        int InnerIdx = DIAMDriver.getInnerIdx();

        int myDist = graph.getPartition(partitionId).getVertexValue(srcNodeIdInPart);

        boolean updateFlag = false;
        int check = 0;
        for (int j = 0; j < neighborListSize; j++) {
            int destId = edges.getQuick(j);
            int destPartitionId = graph.getPartitionId(destId);
            DIAMPartition destPartition = graph.getPartition(destPartitionId);
            int destPosition = graph.getNodePositionInPart(destId);

            int newDist = myDist + weights.getQuick(j);
            boolean updated = destPartition.update(destPosition, newDist);

            if (updated) {
                updateFlag = true;
                int newBucketId = newDist >> delta;
                destPartition.setBucketId(destPosition, newBucketId);
                destPartition.setCurrMaxBucket(newBucketId);
                if (newBucketId == bucketIdx) {
                    destPartition.setInnerIdx(InnerIdx);
                }
            } else {
                if(numCheck != -1 && !updateFlag) {
                    check++;
                    if(check >= numCheck) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void reset() {

    }

    public static void setIsHeavy(boolean value) {
        isHeavy = value;
    }
}
