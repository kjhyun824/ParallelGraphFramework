package algorithm.bfs;

import graph.DirectedGraph;
import graph.GraphAlgorithmInterface;
import graph.GraphPartition;
import graph.NodePartition;
import graph.Node;

public class BFSExecutor implements GraphAlgorithmInterface {

    DirectedGraph graph;
    GraphPartition graphPartition;
    static int currentLevel;
    int numActiveNodes;

    BFSExecutor(DirectedGraph graph) {
        this.graph = graph;
        graphPartition = graph.getPartitionInstance();
    }

    public static void setLevel(int level) {
        currentLevel = level;
    }

    @Override
    public void execute(int partitionId) {
        System.out.print("LEVEL : " + currentLevel + "   ");
        NodePartition partition = graphPartition.getPartition(partitionId);
        int expOfPartitionSize = graphPartition.getExpOfPartitionSize();
        int partitionSize = partition.getSize();
        int offset = partitionId << expOfPartitionSize;
        numActiveNodes = 0;


        for (int i = 0; i < partitionSize; i++) {
            //xxx
            int srcNodeId = offset + i;
            int srcIdPositionInPart = graphPartition.getNodePositionInPart(srcNodeId);
            double srcCurrentLevel = partition.getVertexValue(srcIdPositionInPart);

            if (srcCurrentLevel == currentLevel) {
                Node srcNode = graph.getNode(srcNodeId);

                if (srcNode != null) {
                    update(srcNode);
                }
            }
        }
        System.out.println(numActiveNodes);
        currentLevel++;

    }

    public void update(Node srcNode) {
        int neighborListSize = srcNode.neighborListSize();

        for (int j = 0; j < neighborListSize; j++) {
            int destId = srcNode.getNeighbor(j);
            int destPartitionId = graphPartition.getPartitionId(destId);

            NodePartition destPartition = graphPartition.getPartition(destPartitionId);

            int destIdPositionInPart = graphPartition.getNodePositionInPart(destId);
            double destCurrentLevel = destPartition.getVertexValue(destIdPositionInPart);

            // xxx
            if (destCurrentLevel == 0 || destCurrentLevel > currentLevel + 1) {
                destPartition.update(destIdPositionInPart, currentLevel + 1);
                numActiveNodes++;
            }
        }
    }

    @Override
    public void reset(int taskId) {

    }
}