package graph.partition;

import atomic.AtomicIntegerArray;

public class WCCPartition extends Partition
{
    AtomicIntegerArray nextCompIds;
    int[] curCompIds;
    volatile int updatedEpoch;
    int tryUpdated;
    int actualUpdated;

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
        updatedEpoch = 1;
        tryUpdated = 0;
        actualUpdated = 0;
    }

    public void setUpdatedEpoch(int value) {
        if (updatedEpoch != value) {
            updatedEpoch = value;
        }
    }

    public void incrementTryUpdated() {
        tryUpdated++;
    }

    public void incrementActualUpdated() {
        actualUpdated++;
    }

    public int getTryUpdated() {
        return tryUpdated;
    }

    public int getActualUpdated() {
        return actualUpdated;
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


    public final boolean update(int entry, int value) {
        int prev;
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            prev = nextCompIds.asyncGet(entry);
            if (prev <= value) {
                return false;
            }
            nextCompIds.asyncSet(entry, value);
            return true;
        }
        else {
//            tryUpdate++;
            do {
                prev = nextCompIds.get(entry);      // 40 %

                if (prev <= value) {  // 2%
//                    notUpdated++;
                    return false;                    // 1%
                }
            }
            while (!nextCompIds.compareAndSet(entry, prev, value));     // 1%
            return true;
        }
    }

    public boolean compareCompIds(int prev, int value) {
        return prev > value;
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
//        System.out.println("tryUpdate - notUpdated :  " + (tryUpdate - notUpdated));
    }
}
