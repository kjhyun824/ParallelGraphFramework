package graph.partition;

public abstract class Partition {
    final int numValuesPerNode;
    final int asyncRangeSize;
    int partitionId;
    int partitionSize;
    int tablePos;

    byte partitionActiveValue;
    byte[] activeNodeCheckArray;

    Partition(int partitionId, int maxNodeId, int partitionSize, int numValuesPerNode, int asyncRangeSize) {
        this.partitionId = partitionId;
        this.partitionSize = partitionSize;
        this.partitionActiveValue = 1;
        if ((partitionId + 1) * this.partitionSize > maxNodeId) {
            this.partitionSize = (maxNodeId % partitionSize) + 1;
        }
        this.numValuesPerNode = numValuesPerNode;
        this.asyncRangeSize = asyncRangeSize;
        activeNodeCheckArray = new byte[partitionSize];
        initializeTable();
    }

    public abstract void initializeTable();

    public int getSize() {
        return partitionSize;
    }

    public void setPartitionActiveValue(byte value) {
        if (partitionActiveValue != value) {
            partitionActiveValue = value;
        }
    }

    public boolean checkPartitionIsActive(byte compareValue) {
        return partitionActiveValue == compareValue;
    }

    public boolean checkNodeIsActive(int nodeId, byte compareValue) {
        return activeNodeCheckArray[nodeId] == compareValue;
    }

    public void setIsActive(int nodeId, byte value) {
        activeNodeCheckArray[nodeId] = value;
    }

    public void reset() {
        initializeTable();
        partitionActiveValue = 1;
    }
}
