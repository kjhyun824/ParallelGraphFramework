package graph.partition;

import atomic.AtomicIntegerArray;

import java.util.Arrays;

public class WCCPartition extends Partition
{
    AtomicIntegerArray nextCompIds;
    int[] curCompIds;

    public WCCPartition(int partitionId, int maxNodeId, int partitionSize, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, asyncRangeSize);
    }

    public void initializeTable() {
        nextCompIds = new AtomicIntegerArray(partitionSize);
        curCompIds = new int[partitionSize];
        Arrays.fill(curCompIds, -1);
    }

    public void setNextCompId(int entry, int value) {
        if (entry < asyncRangeSize) {
            nextCompIds.asyncSet(entry, value);
        }
        else {
            nextCompIds.set(entry, value);
        }
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
            if (prev < value) {
                nextCompIds.asyncSet(entry, value);
                return true;
            }
            return false;
        }
        else {
            do {
                prev = nextCompIds.get(entry);
                if (prev >= value) {
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
}