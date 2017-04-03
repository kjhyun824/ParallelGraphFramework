package graph.partition;

import atomic.AtomicIntegerArray;

import java.util.function.IntBinaryOperator;

public class IntegerPartition extends Partition {
    public static IntBinaryOperator updateFunction;

    public static void setUpdateFunction(IntBinaryOperator function) {
        updateFunction = function;
    }

    AtomicIntegerArray[] tables;
    byte[] activeCheckArray;

    public IntegerPartition(int partitionId, int maxNodeId, int partitionSize, int numValuesPerNode, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, numValuesPerNode, asyncRangeSize);
    }

    public final void initializeTable() {
        tables = new AtomicIntegerArray[numValuesPerNode];
        activeCheckArray = new byte[partitionSize];

        for (int i = 0; i < numValuesPerNode; i++) {
            tables[i] = new AtomicIntegerArray(partitionSize);
        }
    }

    public final void initializedCallback() {
        swapConsecutiveTwoTables();
    }

    public final void setVertexValue(int entry, int value) {
        tables[tablePos].set(entry, value);
    }

    public final void setNextVertexValue(int entry, int value) {
        tables[tablePos + 1].set(entry, value);
    }

    public final int getVertexValue(int entry) {
        return tables[tablePos].get(entry);
    }

    public final void update(int entry, int value) {
        update(tablePos, entry, value);
    }

    public final void update(int pos, int entry, int value) {
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            tables[pos].asyncGetAndAccumulate(entry, value, updateFunction);
        }
        else {
            tables[pos].getAndAccumulate(entry, value, updateFunction);
        }
    }

    public final void updateNextTable(int entry, int value) {
        update(tablePos + 1, entry, value);
    }

    public final void swapConsecutiveTwoTables() {
        AtomicIntegerArray tmp = tables[tablePos];
        tables[tablePos] = tables[tablePos + 1];
        tables[tablePos + 1] = tmp;
    }
}
