package graph.partition;

import atomic.AtomicIntegerArray;

public class WCCPartition extends Partition
{
    AtomicIntegerArray nextCompIds;
    int[] curCompIds;
    volatile int updatedEpoch;

    public WCCPartition(int partitionId, int maxNodeId, int partitionSize, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, asyncRangeSize);
    }

    public void initializeTable() {
        nextCompIds = new AtomicIntegerArray(partitionSize);
        curCompIds = new int[partitionSize];

        int offset = partitionId * partitionSize;
        for (int i = 0; i < partitionSize; i++) {
            curCompIds[i] = -1;
            nextCompIds.set(i, offset + i);
        }
        updatedEpoch = 0;
    }

    public void setUpdatedEpoch(int value) {
        if (updatedEpoch != value) {
            updatedEpoch = value;
        }
    }

    public int getUpdatedEpoch() {
        return updatedEpoch;
    }

    public int getNextCompId(int entry) {
        if (entry < asyncRangeSize) {
            return nextCompIds.asyncGet(entry);
        }
        else {
            return nextCompIds.get(entry);
        }
    }

    //    volatile int tryUpdate = 0;
//    volatile int notUpdated = 0;
    public final boolean update(int entry, int value) {
        int prev;
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            prev = nextCompIds.asyncGet(entry);
            if (prev > value) {
                nextCompIds.asyncSet(entry, value);
                return true;
            }
            return false;
        }
        else {
//            tryUpdate++;
            do {
                prev = nextCompIds.get(entry);
                if (prev <= value) {
//                    notUpdated++;
                    return false;
                }
            }
            while (!nextCompIds.compareAndSet(entry, prev, value));
            return true;
        }
    }

    public void setCurComponentId(int pos, int value) {
        curCompIds[pos] = value;
    }

    public int getCurCompId(int pos) {
        return curCompIds[pos];
    }

    public void reset() {
        initializeTable();
        partitionActiveValue = 0;
        updatedEpoch = 0;

//        System.out.println("tryUpdate = "+tryUpdate+", notUpdated:"+notUpdated);
    }
}