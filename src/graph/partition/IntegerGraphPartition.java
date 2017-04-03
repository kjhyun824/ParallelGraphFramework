package graph.partition;

import graph.DirectedGraph;

public class IntegerGraphPartition {
    static IntegerGraphPartition instance = null;
    DirectedGraph graph;
    IntegerNodePartition [] partitions;
    final int expOfPartitionSize;
    final int partitionCapacity;
    final int bitMaskForRemain;
    final int numPartitions;

    IntegerGraphPartition(DirectedGraph graph, int expOfPartitionSize) {
        this.graph = graph;
        this.expOfPartitionSize = expOfPartitionSize;
        this.partitionCapacity = 1 << expOfPartitionSize;
        bitMaskForRemain = (1 << expOfPartitionSize) - 1;

        int nodeCapacity = graph.getMaxNodeId() + 1; // TODO : Change Capacity to the number of node
        numPartitions = (nodeCapacity + partitionCapacity - 1) / partitionCapacity;
    }

    public static IntegerGraphPartition getInstance(DirectedGraph graph, int expOfPartitionSize) {
        if (instance == null) {
            instance = new IntegerGraphPartition(graph, expOfPartitionSize);
        }
        return instance;
    }

    public void generate(int numValuesPerNode, int asyncRangeSize) {
        partitions = new IntegerNodePartition[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = new IntegerNodePartition(i, graph.getMaxNodeId(), partitionCapacity, numValuesPerNode, asyncRangeSize);
        }
    }

    public int getExpOfPartitionSize() {
        return expOfPartitionSize;
    }

    public IntegerNodePartition[] getPartitions() {
        return partitions;
    }

    public IntegerNodePartition getPartition(int partitionId) {
        return partitions[partitionId];
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

