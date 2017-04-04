package graph;

import graph.partition.DoublePartition;
import graph.partition.IntegerPartition;

import java.lang.reflect.Array;

public abstract class Graph<T> {
    final static int defaultSize = 10;
    static DirectedGraph instance = null;

    Node[] tmpNodes;
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

    Graph(int expOfPartitionSize) {
        this.expOfPartitionSize = expOfPartitionSize;
        partitionCapacity = 1 << expOfPartitionSize;
        bitMaskForRemain = (1 << expOfPartitionSize) - 1;
        nodes = new Node[defaultSize];
    }

    abstract boolean addEdge(int srcNodeId, int destNodeId);

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

    public void finalizeLoading() {
        tmpNodes = null;
    }




    // The following part is related to Partiton

    public void generatePartition(int numValuesPerNode, int asyncRangeSize, Class<T> partitionClass) {
        int nodeCapacity = maxNodeId + 1; // TODO : Change Capacity to the number of node
        numPartitions = (nodeCapacity + (partitionCapacity - 1)) / partitionCapacity;
        partitions = (T[]) Array.newInstance(partitionClass, numPartitions);

        if (partitionClass == IntegerPartition.class) {
            for (int i = 0; i < numPartitions; i++) {
                partitions[i] = (T) new IntegerPartition(i, maxNodeId, partitionCapacity, numValuesPerNode, asyncRangeSize);
            }
        }
        else if (partitionClass == DoublePartition.class) {
            for (int i = 0; i < numPartitions; i++) {
                partitions[i] = (T) new DoublePartition(i, maxNodeId, partitionCapacity, numValuesPerNode, asyncRangeSize);
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
}

