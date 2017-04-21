package graph.partition;

import atomic.AtomicIntegerArray;

import java.util.Arrays;
import java.util.function.IntBinaryOperator;

public class IntegerPartition extends Partition
{
    public static IntBinaryOperator updateFunction;

    public static void setUpdateFunction(IntBinaryOperator function) {
        updateFunction = function;
    }

    AtomicIntegerArray table;

    public IntegerPartition(int partitionId, int maxNodeId, int partitionSize, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, asyncRangeSize);
    }

    public final void initializeTable() {
        table = new AtomicIntegerArray(partitionSize);
        activeNodeCheckArray = new int[partitionSize];

        Arrays.fill(activeNodeCheckArray, -1);
    }

    public final void setVertexValue(int entry, int value) {
        if (entry < asyncRangeSize) {
            table.asyncSet(entry, value);
        }
        else {
            table.set(entry, value);
        }
    }

    public final int getVertexValue(int entry) {
        if (entry < asyncRangeSize) {
            return table.asyncGet(entry);
        }
        else {
            return table.get(entry);
        }
    }

    public final void update(int entry, int value) {
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            table.asyncGetAndAccumulate(entry, value, updateFunction);
        }
        else {
            table.getAndAccumulate(entry, value, updateFunction);
        }
    }
}

