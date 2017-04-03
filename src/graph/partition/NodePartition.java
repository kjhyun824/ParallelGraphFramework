package graph.partition;

public abstract class NodePartition {
    final int numValuesPerNode;
    final int asyncRangeSize;
    int partitionId;
    int partitionSize;
    int tablePos;

    byte level;

    NodePartition(int partitionId, int maxNodeId, int partitionSize, int numValuesPerNode, int asyncRangeSize) {
        this.partitionId = partitionId;
        this.partitionSize = partitionSize;
        this.level = 1;
        if ((partitionId + 1) * this.partitionSize > maxNodeId) {
            this.partitionSize = (maxNodeId % partitionSize) + 1;
        }
        this.numValuesPerNode = numValuesPerNode;
        this.asyncRangeSize = asyncRangeSize;

        initializeTable();
    }

    public abstract void initializeTable();

}
