package graph;

public class UnDirectedGraph extends Graph {
    static UnDirectedGraph instance = null;

    public static UnDirectedGraph getInstance(int expOfPartitionSize) {
        if (instance == null) {
            instance = new UnDirectedGraph(expOfPartitionSize);
        }
        return instance;
    }

    public UnDirectedGraph(int expOfPartitionSize) {
        super(expOfPartitionSize);
    }

    public boolean addEdge(int srcNodeId, int destNodeId) {
        checkAndCreateNodes(srcNodeId, destNodeId);

        Node srcNode = nodes[srcNodeId];
        Node destNode = nodes[destNodeId];

        boolean isAdded = srcNode.addNeighborId(destNodeId); // Do not allow duplication

        if (isAdded) {
            destNode.addNeighborId(srcNodeId);
            srcNode.incrementInDegree();
            srcNode.incrementOutDegree();
            destNode.incrementInDegree();
            destNode.incrementOutDegree();
            numEdges++;
        }

        return isAdded;
    }
}

