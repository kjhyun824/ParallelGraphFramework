package algorithm.bfs;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.GraphPartition;
import graph.NodePartition;
import graph.Node;

public class BFSExecutor implements GraphAlgorithmInterface {

    DirectedGraph graph;
    GraphPartition graphPartition;
    NodePartition partition;
    static int currentLevel;
    int numActivatedNodes;

    BFSExecutor(DirectedGraph graph) {
        this.graph = graph;

        graphPartition = graph.getPartitionInstance();
    }

    public static void setLevel(int level) {
        currentLevel = level;
    }

    public static int getLevel() {
        return currentLevel;
    }

    @Override
    public void execute(int partitionId) {
        partition = graphPartition.getPartition(partitionId);
        int partitionSize = partition.getSize();
        int expOfPartitionSize = graphPartition.getExpOfPartitionSize();
        int offset = partitionId << expOfPartitionSize;

        numActivatedNodes = 0;
        for (int i = 0; i < partitionSize; i++) {
            int nodeId = offset + i;
            int nodePositionInPart = graphPartition.getNodePositionInPart(nodeId);

            if (partition.getVertexValue(nodePositionInPart) == currentLevel) {
                Node srcNode = graph.getNode(nodeId);

                if (srcNode != null) {
                    update(srcNode);
                }
            }
        }

        BFSDriver.addTotalActiveNodes(numActivatedNodes);
    }

    public void update(Node srcNode) {
        int neighborListSize = srcNode.neighborListSize();

        for (int j = 0; j < neighborListSize; j++) {
            int destId = srcNode.getNeighbor(j);
            int destPartitionId = graphPartition.getPartitionId(destId);
            NodePartition destPartition = graphPartition.getPartition(destPartitionId);
            int destPosition = graphPartition.getNodePositionInPart(destId);
            double destLevel = destPartition.getVertexValue(destPosition);    //vertexValue is level

            if (destLevel == 0) {
                //Activate Node
                destPartition.update(destPosition, currentLevel + 1);
                numActivatedNodes++;
            }
        }

        //System.out.println("[DEBUG] NumActivated = " + numActivatedNodes);
    }

    @Override
    public void reset(int taskId) {

    }
}