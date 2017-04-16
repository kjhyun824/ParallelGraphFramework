package algorithm.sssp;

import gnu.trove.list.array.TIntArrayList;
import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.partition.SSSPPartition;

public class SSSPExecutor implements GraphAlgorithmInterface
{
    Graph<SSSPPartition> graph;
    SSSPPartition partition;
    TIntArrayList[] lightEdges;
    TIntArrayList[] heavyEdges;
    TIntArrayList[] edges = null;
    Node srcNode;

    static boolean isHeavy;
    static int bucketId;

    int offset;
    int partitionSize;
    int partitionId;
    double delta;

    public static void setCurBucketId(int id) {
        bucketId = id;
    }

    SSSPExecutor(int partitionId, Graph<SSSPPartition> graph, double delta) {
        this.partitionId = partitionId;
        this.graph = graph;
        this.delta = delta;
        this.lightEdges = SSSPDriver.getLightEdges();
        this.heavyEdges = SSSPDriver.getHeavyEdges();
        this.offset = this.partitionId << graph.getExpOfPartitionSize();
        this.partition = graph.getPartition(partitionId);
        this.partitionSize = partition.getSize();
    }

    @Override
    public void execute() {
        if (!isHeavy) {
            edges = lightEdges;
        }
        else {
            edges = heavyEdges;
        }

        for (int i = 0; i < partitionSize; i++) {
            int srcId = offset + i;
            int currBucket = partition.getBucketIds(i);

            if (currBucket == bucketId) {
                srcNode = graph.getNode(srcId);
                if (srcNode != null) {
                    update(i);
                }
            }
        }
    }

    public void update(int srcNodeIdInPart) {
        TIntArrayList srcEdges = edges[srcNodeIdInPart];

        int neighborListSize = srcEdges.size();

        for (int j = 0; j < neighborListSize; j++) {
            int destId = srcEdges.get(j);
            int destPartitionId = graph.getPartitionId(destId);
            SSSPPartition destPartition = graph.getPartition(destPartitionId);
            int destPosition = graph.getNodePositionInPart(destId);

            // xxx :
            // (graph.getNode(destOffset + j) == srcNode) -> (graph.getNode(destId) == srcNode)
            // reason : destOffset + j != destId

            if (graph.getNode(destId) == srcNode) { // Consider Self Edge
                continue;
            }

            // xxx : graph.getPartition(partitionId) == partition
            double newDist = partition.getVertexValue(srcNodeIdInPart) + srcNode.getNeighborWeight(destId);
            double distTent = destPartition.getVertexValue(destPosition);

            if (newDist == distTent) {
                break;
            }

            int newBucketId = ((int) Math.floor(newDist / delta));

            destPartition.update(destPosition, newDist);
            // xxx :
            // destPositionId -> destPartition
            destPartition.setBucketIds(destPosition, newBucketId);

            if (newBucketId == bucketId) {
                System.out.println("[DEBUG] SRCPart->DESTPart : " + partitionId + "->" + destPartitionId + " / srcId->DestId" + (offset + srcNodeIdInPart) + "->" + (destId) + " currDist : " + newDist + " newBucketId : " + newBucketId + " Curr Bucket Id : " + SSSPDriver.getBucketIdx() + " Inner Idx " + SSSPDriver.getInnerIdx());
                destPartition.setInnerIter(SSSPDriver.getInnerIdx());
            }

            if (newBucketId > destPartition.getCurrMaxBucket()) {
                destPartition.setCurrMaxBucket(newBucketId);
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