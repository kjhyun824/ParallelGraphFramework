package graph.partition;

public abstract class Partition {
    final int numValuesPerNode;
    final int asyncRangeSize;
    int partitionId;
    int partitionSize;
    int tablePos;

    int partitionActiveValue;
    int[] activeNodeCheckArray;


    Partition(int partitionId, int maxNodeId, int partitionSize, int numValuesPerNode, int asyncRangeSize) {
        this.partitionId = partitionId;
        this.partitionSize = partitionSize;
        this.partitionActiveValue = 1;
        if ((partitionId + 1) * this.partitionSize > maxNodeId) {
            this.partitionSize = (maxNodeId % partitionSize) + 1;
        }
        this.numValuesPerNode = numValuesPerNode;
        this.asyncRangeSize = asyncRangeSize;
        activeNodeCheckArray = new int[partitionSize];
        initializeTable();
    }

    public abstract void initializeTable();

    public int getSize() {
        return partitionSize;
    }

    public void setPartitionActiveValue(int value) {
        if (partitionActiveValue != value) {
            partitionActiveValue = value;
        }
    }

    public boolean checkPartitionIsActive(int compareValue) {
        return partitionActiveValue == compareValue;
    }

    public int getPartitionActiveValue() {
        return partitionActiveValue;
    }

    public int getNodeActiveValue(int pos) {
        return activeNodeCheckArray[pos];
    }

    public void setNodeIsActive(int pos, int value) {
        if (activeNodeCheckArray[pos] != value) {
            activeNodeCheckArray[pos] = value;
        }
    }

    public void reset() {
        initializeTable();
        partitionActiveValue = 1;
    }
}
