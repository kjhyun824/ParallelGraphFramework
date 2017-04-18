package graph.partition;

import atomic.AtomicByteArray;
import atomic.AtomicDoubleArray;
import atomic.AtomicIntegerArray;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleBinaryOperator;

public class SSSPPartition extends Partition
{
    public static DoubleBinaryOperator updateFunction;

    AtomicDoubleArray tables;
    AtomicIntegerArray bucketIds;
    AtomicInteger currMaxBucket;
    int innerIdx;

    public SSSPPartition(int partitionId, int maxNodeId, int partitionSize, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, asyncRangeSize);
    }

    public final void initializeTable() {
        tables = new AtomicDoubleArray(partitionSize);
        bucketIds = new AtomicIntegerArray(partitionSize);
        currMaxBucket = new AtomicInteger();

        for (int i = 0; i < partitionSize; i++) {
            tables.set(i, Double.POSITIVE_INFINITY);
            bucketIds.set(i, Integer.MAX_VALUE);
        }
        innerIdx = -1;
        currMaxBucket.set(-1);
    }

    public void setBucketId(int entry, int newId) {
        int prevId;
        if (entry < asyncRangeSize) {
            prevId = bucketIds.get(entry);
            if (prevId > newId) {
                bucketIds.asyncSet(entry, newId);
            }
        }
        else {
            do {
                prevId = bucketIds.get(entry);
                if (prevId <= newId) {
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
        int currMax;
        do {
            currMax = currMaxBucket.get();
            if (currMax >= value) {
                break;
            }
        }
        while (!currMaxBucket.compareAndSet(currMax, value));
    }

    public int getCurrMaxBucket() {
        return currMaxBucket.get();
    }

    public final double getVertexValue(int entry) {
        if (entry < asyncRangeSize) {
            return tables.asyncGet(entry);
        }
        else {
            return tables.get(entry);
        }
    }

    public final void update(int entry, double newDist) {
        double currDist;
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            currDist = tables.asyncGet(entry);
            if (currDist == 0 || currDist > newDist) {
                tables.asyncGetAndAccumulate(entry, newDist, updateFunction);
            }
        }
        else {
            do {
                currDist = tables.get(entry);
                if (currDist <= newDist) {
                    break;
                }
            }
            while (!tables.compareAndSet(entry, currDist, newDist));
        }
    }

    public void reset() {
        initializeTable();
        partitionActiveValue = 1;
    }
}