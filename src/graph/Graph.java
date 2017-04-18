package graph;

import graph.partition.PageRankPartition;
import graph.partition.IntegerPartition;
import graph.partition.SSSPPartition;
import graph.partition.WCCPartition;

import java.lang.reflect.Array;

public class Graph<T>
{
    final static int defaultSize = 10;
    static Graph instance = null;

    boolean isDirected;
    boolean isWeighted;

    //    Node[] tmpNodes;
    Node[] nodes;

    int numNodes;
    int numEdges;
    int maxNodeId;

    // related to partition
    T[] partitions;
    final int expOfPartitionSize;
    final int partitionCapacity;
    final int bitMaskForRemain;

    int numPartitions;

    Graph(int expOfPartitionSize, boolean isDirected, boolean isWeighted) {
        this.expOfPartitionSize = expOfPartitionSize;
        this.isDirected = isDirected;
        this.isWeighted = isWeighted;
        partitionCapacity = 1 << expOfPartitionSize;
        bitMaskForRemain = (1 << expOfPartitionSize) - 1;

        nodes = new Node[defaultSize];
    }

    public static Graph getInstance(int expOfPartitionSize, boolean isDirected, boolean isWeighted) {
        if (instance == null) {
            instance = new Graph(expOfPartitionSize, isDirected, isWeighted);
        }
        return instance;
    }

    public boolean addEdge(int srcNodeId, int destNodeId) {
        checkAndCreateNodes(srcNodeId, destNodeId);

        Node srcNode = nodes[srcNodeId];
        Node destNode = nodes[destNodeId];

        boolean isAdded = srcNode.addNeighborId(destNodeId); // Do not allow duplication

        if (isAdded) {
            if (isDirected) {
                srcNode.incrementOutDegree();
                destNode.incrementInDegree();
                numEdges++;
            }
            else {
                destNode.addNeighborId(srcNodeId);
                srcNode.incrementInDegree();
                srcNode.incrementOutDegree();
                destNode.incrementInDegree();
                destNode.incrementOutDegree();
                numEdges++;
            }
        }

        return isAdded;
    }

    public boolean addEdge(int srcNodeId, int destNodeId, double weight) {
        checkAndCreateNodes(srcNodeId, destNodeId);

        Node srcNode = nodes[srcNodeId];
        Node destNode = nodes[destNodeId];

        boolean isAdded = srcNode.addNeighborId(destNodeId, weight);

        if (isAdded) {
            if (isDirected) {
                srcNode.incrementOutDegree();
                destNode.incrementInDegree();
                numEdges++;
            }
            else {
                destNode.addNeighborId(srcNodeId, weight);
                srcNode.incrementInDegree();
                srcNode.incrementOutDegree();
                destNode.incrementInDegree();
                destNode.incrementOutDegree();
                numEdges++;
            }
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

    // The following part is related to Partiton

    public void generatePartition(int asyncRangeSize, Class<T> partitionClass) {
        int nodeCapacity = maxNodeId + 1; // TODO : Change Capacity to the number of node
        numPartitions = (nodeCapacity + (partitionCapacity - 1)) / partitionCapacity;
        partitions = (T[]) Array.newInstance(partitionClass, numPartitions);

        if (partitionClass == IntegerPartition.class) {
            for (int i = 0; i < numPartitions; i++) {
                partitions[i] = (T) new IntegerPartition(i, maxNodeId, partitionCapacity, asyncRangeSize);
            }
        }
        else if (partitionClass == PageRankPartition.class) {
            for (int i = 0; i < numPartitions; i++) {
                partitions[i] = (T) new PageRankPartition(i, maxNodeId, partitionCapacity, asyncRangeSize);
            }
        }
        else if (partitionClass == SSSPPartition.class) {
            for (int i = 0; i < numPartitions; i++) {
                partitions[i] = (T) new SSSPPartition(i, maxNodeId, partitionCapacity, asyncRangeSize);
            }
        }
        else if (partitionClass == WCCPartition.class) {
            for (int i = 0; i < numPartitions; i++) {
                partitions[i] = (T) new WCCPartition(i, maxNodeId, partitionCapacity, asyncRangeSize);
            }
        }
    }

    public T[] getPartitions() {
        return partitions;
    }

    public T getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public int getExpOfPartitionSize() {
        return expOfPartitionSize;
    }

    public int getNumPartitions() {
        return partitions.length;
    }

    public int getPartitionId(int nodeId) {
        //  = nodeNumber / partitionCapacity
        return nodeId >> expOfPartitionSize;
    }

    public int getNodePositionInPart(int nodeId) {
        //  = nodeNumber % partitionCapacity
        return nodeId & bitMaskForRemain;
    }

    public int getNodeNumberInPart(int partitionNumber, int position) {
        return (partitionNumber << expOfPartitionSize) + position;
    }

    public boolean isDirected() {
        return isDirected;
    }

    public boolean isWeighted() {
        return isWeighted;
    }
}