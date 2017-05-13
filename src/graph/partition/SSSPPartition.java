package graph.partition;

import atomic.AtomicIntegerArray;

public class SSSPPartition extends Partition
{
    AtomicIntegerArray tables;
    AtomicIntegerArray bucketIds;
    int currMaxBucket;
    volatile int innerIdx;

    public SSSPPartition(int partitionId, int maxNodeId, int partitionSize, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, asyncRangeSize);
    }

    public final void initializeTable() {
        tables = new AtomicIntegerArray(partitionSize);
        bucketIds = new AtomicIntegerArray(partitionSize);

        for (int i = 0; i < partitionSize; i++) {
            tables.set(i, Integer.MAX_VALUE);
            bucketIds.set(i, -1);
        }
        innerIdx = -1;
        currMaxBucket = -1;
    }

    public void setBucketId(int entry, int newId) {
        int prevId;
        if (entry < asyncRangeSize) {
            prevId = bucketIds.get(entry);
            if (prevId == -1 || prevId > newId) {
                bucketIds.asyncSet(entry, newId);
            }
        }
        else {
            do {
                prevId = bucketIds.get(entry);
                if (prevId != -1 && newId <= prevId) {
                    break;
                }
            }
            while (!bucketIds.compareAndSet(entry, prevId, newId));
        }
    }

    public int getBucketId(int entry) {
        return bucketIds.get(entry);
    }

    public void setInnerIdx(int value) {
        if (innerIdx != value) {
            innerIdx = value;
        }
    }

    public int getInnerIdx() {
        return innerIdx;
    }

    public void setCurrMaxBucket(int value) {
        if (currMaxBucket < value) {
            currMaxBucket = value;
        }
    }

    public int getCurrMaxBucket() {
        return currMaxBucket;
    }

    public final int getVertexValue(int entry) {
        if (entry < asyncRangeSize) {
            return tables.asyncGet(entry);
        }
        else {
            return tables.get(entry);
        }
    }

    public final boolean update(int entry, int newDist) {
        int currDist;
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            currDist = tables.asyncGet(entry);
            if (newDist < currDist) {
                tables.asyncSet(entry, newDist);
                return true;
            }
            return false;
        }
        else {
            do {
                currDist = tables.get(entry);
                if (newDist >= currDist) {
                    return false;
                }
            }
            while (!tables.compareAndSet(entry, currDist, newDist));
            return true;
        }
    }

    public void reset() {
        initializeTable();
        partitionActiveValue = 1;
    }
}