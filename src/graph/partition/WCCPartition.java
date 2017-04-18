package graph.partition;

import atomic.AtomicIntegerArray;

import java.util.Arrays;
import java.util.function.IntBinaryOperator;

public class WCCPartition extends Partition
{
    public static IntBinaryOperator updateFunction;

    public static void setUpdateFunction(IntBinaryOperator function) {
        updateFunction = function;
    }

    AtomicIntegerArray tables;

    public WCCPartition(int partitionId, int maxNodeId, int partitionSize, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, asyncRangeSize);
    }

    public void initializeTable() {
        tables = new AtomicIntegerArray(partitionSize);
        activeNodeCheckArray = new int[partitionSize];
        Arrays.fill(activeNodeCheckArray, -1);
    }

    public void setVertexValue(int entry, int value) {
        if (entry < asyncRangeSize) {
            tables.asyncSet(entry, value);
        }
        else {
            tables.set(entry, value);
        }
    }

    public int getVertexValue(int entry) {
        if (entry < asyncRangeSize) {
            return tables.asyncGet(entry);
        }
        else {
            return tables.get(entry);
        }
    }

    public void update(int entry, int value) {
        int prev;
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            prev = tables.asyncGet(entry);
            if (value > prev) {
                tables.asyncSet(entry, value);
            }
        }
        else {
            do {
                prev = tables.get(entry);
                if (value <= prev) {
                    break;
                }
            }
            while (!tables.compareAndSet(entry, prev, value));
        }
    }
}