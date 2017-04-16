package graph.partition;

import atomic.AtomicByteArray;
import atomic.AtomicDoubleArray;
import atomic.AtomicIntegerArray;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleBinaryOperator;

public class SSSPPartition extends Partition {
    public static DoubleBinaryOperator updateFunction;

    public static void setUpdateFunction(DoubleBinaryOperator function) {
        updateFunction = function;
    }

    AtomicDoubleArray tables;
    AtomicIntegerArray bucketIds;
    AtomicInteger innerIter;
    AtomicInteger currMaxBucket;

    public SSSPPartition(int partitionId, int maxNodeId, int partitionSize, int numValuesPerNode, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, numValuesPerNode, asyncRangeSize);
    }

    public final void initializeTable() {
        tables = new AtomicDoubleArray(partitionSize);
        activeNodeCheckArray = new int[partitionSize];
        bucketIds = new AtomicIntegerArray(partitionSize);
        innerIter = new AtomicInteger();
        currMaxBucket = new AtomicInteger();

        Arrays.fill(activeNodeCheckArray, -1);

        for (int i = 0; i < partitionSize; i++) {
            tables.set(i,0);
            bucketIds.set(i, -1);
        }
        innerIter.set(-1);
        currMaxBucket.set(-1);
    }

    public void setBucketIds(int entry, int value) {
        int temp;
        do {
            temp = bucketIds.get(entry);
            if (temp != -1 && temp <= value) break;
        } while (!bucketIds.compareAndSet(entry, temp, value));
    }

    public int getBucketIds(int entry) {
        return bucketIds.get(entry);
    }

    public void setInnerIter(int value) {
        if (innerIter.get() != value)
            innerIter.set(value);
    }

    public int getInnerIter() {
        return innerIter.get();
    }

    public void setCurrMaxBucket(int value) {
        int temp;
        do {
            temp = currMaxBucket.get();
            if (temp >= value) break;
        } while (!currMaxBucket.compareAndSet(temp, value));
    }

    public int getCurrMaxBucket() {
        return currMaxBucket.get();
    }

    public final double getVertexValue(int entry) {
        if (entry < asyncRangeSize) {
            return tables.asyncGet(entry);
        } else {
            return tables.get(entry);
        }
    }

    public final void update(int entry, double value) {
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            if (tables.asyncGet(entry) > value) {
                tables.asyncGetAndAccumulate(entry, value, updateFunction);
            }
        } else {
            double tempDist;
            do {
                tempDist = tables.get(entry);
                if (tempDist != 0 && tempDist <= value) break;
            } while (!tables.compareAndSet(entry, tempDist, value));
        }
    }

    public void reset() {
        initializeTable();
        partitionActiveValue = 1;
    }
}