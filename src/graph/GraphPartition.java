package graph;

import gnu.trove.list.array.TIntArrayList;

import java.util.HashMap;

public class GraphPartition {
    DirectedGraph graph;
    NodePartition[] partitions;
    final int expOfPartitionSize;
    final int partitionCapacity;
    final int bitMaskForRemain;
    final int numPartitions;

    protected GraphPartition(DirectedGraph graph, int expOfPartitionSize) {
        this.graph = graph;
        this.expOfPartitionSize = expOfPartitionSize;
        this.partitionCapacity = 1 << expOfPartitionSize;
        bitMaskForRemain = (1 << expOfPartitionSize) - 1;

        int nodeCapacity = graph.getMaxNodeId() + 1; // TODO : Change Capacity to the number of node
        numPartitions = (nodeCapacity + partitionCapacity - 1) / partitionCapacity;

        partitions = new NodePartition[numPartitions];
    }

    public void generate(int numValuesPerNode, int asyncRangeSize) {
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = new NodePartition(i, graph.getMaxNodeId(), partitionCapacity, numValuesPerNode, asyncRangeSize);
        }
    }



    public int getPartitionCapacity() {
        return partitionCapacity;
    }

    public NodePartition[] getPartitions() {
        return partitions;
    }

    public NodePartition getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public int getNumPartitions() {
        return partitions.length;
    }

    public int getPartitionNumber(int nodeId) {
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

