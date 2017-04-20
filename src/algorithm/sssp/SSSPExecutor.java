package algorithm.sssp;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.SSSPPartition;

public class SSSPExecutor implements GraphAlgorithmInterface
{

    Graph<SSSPPartition> graph;
    SSSPPartition partition;
    int offset;
    int delta;
    static volatile boolean isHeavy;

    TIntArrayList[] lightEdges;
    TIntArrayList[] lightWeights;
    TIntArrayList[] heavyEdges;
    TIntArrayList[] heavyWeights;
    TIntArrayList edges = null;
    TIntArrayList weights = null;

    final int partitionId;

    SSSPExecutor(int partitionId, Graph<SSSPPartition> graph, int delta) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.delta = delta;
        this.lightEdges = SSSPDriver.getLightEdges();
        this.heavyEdges = SSSPDriver.getHeavyEdges();
        this.lightWeights = SSSPDriver.getLightWeights();
        this.heavyWeights = SSSPDriver.getHeavyWeights();
        this.offset = this.partitionId << graph.getExpOfPartitionSize();
    }

    @Override
    public void execute() {
        partition = graph.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int bucketIdx = SSSPDriver.getBucketIdx();

        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            int currBucket = partition.getBucketId(i);
            if (currBucket == bucketIdx) {
                Node srcNode = graph.getNode(nodeId);

                if (srcNode == null) {
                    continue;
                }

                update(null, i, bucketIdx);
            }
        }
    }

    public void update(Node srcNode, int srcNodeIdInPart, int bucketIdx) {
        if (!isHeavy) {
            edges = lightEdges[offset + srcNodeIdInPart];
            weights = lightWeights[offset + srcNodeIdInPart];
        }
        else {
            edges = heavyEdges[offset + srcNodeIdInPart];
            weights = heavyWeights[offset + srcNodeIdInPart];
        }

        int neighborListSize = edges.size();
        int InnerIdx = SSSPDriver.getInnerIdx();

        int mydist = graph.getPartition(partitionId).getVertexValue(srcNodeIdInPart);

        for (int j = 0; j < neighborListSize; j++) {
            int destId = edges.getQuick(j);
            int destPartitionId = graph.getPartitionId(destId);
            SSSPPartition destPartition = graph.getPartition(destPartitionId);
            int destPosition = graph.getNodePositionInPart(destId);

            int newDist = mydist + weights.getQuick(j);
            boolean updated = destPartition.update(destPosition, newDist);

            if (updated) {
                int newBucketId = newDist >> delta;
                destPartition.setBucketId(destPosition, newBucketId);
                destPartition.setCurrMaxBucket(newBucketId);
                if (newBucketId == bucketIdx) {
                    destPartition.setInnerIdx(InnerIdx);
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