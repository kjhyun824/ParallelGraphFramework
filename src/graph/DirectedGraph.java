package graph;

public class DirectedGraph <T> {
    final static int defaultSize = 10;
    static DirectedGraph instance = null;
    T graphPartition = null;

    Node[] tmpNodes;
    Node[] nodes;

    int numNodes;
    int numEdges;
    int maxNodeId;

    public static DirectedGraph getInstance() {
        if (instance == null) {
            instance = new DirectedGraph();
        }
        return instance;
    }

    DirectedGraph() {
        nodes = new Node[defaultSize];
    }

    public boolean addEdge(int srcNodeId, int destNodeId) {
        checkAndCreateNodes(srcNodeId, destNodeId);

        Node srcNode = nodes[srcNodeId];
        Node destNode = nodes[destNodeId];

        boolean isAdded = srcNode.addNeighborId(destNodeId); // Do not allow duplication

        if (isAdded) {
            srcNode.incrementOutDegree();
            destNode.incrementInDegree();
            numEdges++;
        }

        return isAdded;
    }

    void checkAndCreateNodes(int srcNodeId, int destNodeId) {
        int biggerNodeId = Math.max(srcNodeId, destNodeId);
        if (biggerNodeId > maxNodeId) {
            setMaxNodeId(biggerNodeId);
        }
        ensureNodesCapacity(biggerNodeId + 1);
        ensureNodeEntry(srcNodeId);
        ensureNodeEntry(destNodeId);
    }

    void ensureNodesCapacity(int capacity) { // TODO : vertex ID may not start with 1 but 10,000,000
        if (capacity > nodes.length) {
            int newCapacity = Math.max(nodes.length << 1, capacity);
            Node[] tmp = new Node[newCapacity];
            System.arraycopy(nodes, 0, tmp, 0, nodes.length);
            nodes = tmp;
        }
    }

    void ensureNodeEntry(int entry) {
        Node node = nodes[entry];
        if (node == null) {
            node = new Node();
            nodes[entry] = node;
            numNodes++;
        }
    }

    public void generateTransposeEdges() {
        for (int i = 0; i <= maxNodeId; i++) {
            if (nodes[i] != null) {
                int neighborListSize = nodes[i].neighborListSize();

                for (int j = 0; j < neighborListSize; j++) {
                    int neighborNodeId = nodes[i].getNeighbor(j);
                    nodes[neighborNodeId].addReverseEdge(i);
                }
            }
        }
    }

    public Node getNode(int nodeId) {
        return nodes[nodeId];
    }

    public int getNumNodes() {
        return numNodes;
    }

    public void setMaxNodeId(int nodeId) {
        this.maxNodeId = nodeId;
    }

    public int getMaxNodeId() {
        return maxNodeId;
    }

    public T getPartitionInstance () {
        if (graphPartition == null) {
            //TODO : if graphPartition is null, we have some action.
        }
        return graphPartition;
    }

    public void setGraphPartition(T graphPartition) {
        this.graphPartition = graphPartition;
    }

    public void finalizeLoading() {
        tmpNodes = null;
    }
}

