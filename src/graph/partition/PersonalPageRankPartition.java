package graph.partition;

import atomic.AtomicDoubleArray;
import java.util.function.DoubleBinaryOperator;

public class PersonalPageRankPartition extends Partition
{
    public static DoubleBinaryOperator updateFunction;

    public static void setUpdateFunction(DoubleBinaryOperator function) {
        updateFunction = function;
    }

    AtomicDoubleArray[] tables;
    byte[] actives;

    int tablePos = 0;

    public PersonalPageRankPartition(int partitionId, int maxNodeId, int partitionSize, int asyncRangeSize) {
        super(partitionId, maxNodeId, partitionSize, asyncRangeSize);
    }

    public final void initializeTable() {
        actives = new byte[partitionSize];
        tables = new AtomicDoubleArray[2];
        for (int i = 0; i < tables.length; i++) {
            tables[i] = new AtomicDoubleArray(partitionSize);
        }
    }

    public final void initializedCallback() {
        swapConsecutiveTwoTables();
    }

    public void setActive(int pos) {
        if (actives[pos] == 0) {
            actives[pos] = 1;
        }
    }

    public boolean isNodeActive(int pos) {
        return actives[pos] == 1;
    }

    public final int getActiveNumNodes() {
        int activeNumNodes = 0;
        for (int i = 0; i < actives.length; i++) {
            if (actives[i] == 1) {
                activeNumNodes++;
            }
        }
        return activeNumNodes;
    }

    public final void setVertexValue(int entry, double value) {
        if (entry < asyncRangeSize) {
            tables[tablePos].asyncSet(entry, value);
        }
        else {
            tables[tablePos].set(entry, value);
        }
    }

    public final void setNextVertexValue(int entry, double value) {
        if (entry < asyncRangeSize) {
            tables[tablePos + 1].asyncSet(entry, value);
        }
        else {
            tables[tablePos + 1].set(entry, value);
        }
    }

    public final double getVertexValue(int entry) {
        if (entry < asyncRangeSize) {
            return tables[tablePos].asyncGet(entry);
        }
        else {
            return tables[tablePos].get(entry);
        }
    }

    public final void update(int entry, double value) {
        update(tablePos, entry, value);
    }

    public final void update(int pos, int entry, double value) {
        if (entry < asyncRangeSize) { // TODO : think about multiple ranges in a single partition
            tables[pos].asyncGetAndAccumulate(entry, value, updateFunction);
        }
        else {
            tables[pos].getAndAccumulate(entry, value, updateFunction);
        }
    }

    public final void updateNextTable(int entry, double value) {
        update(tablePos + 1, entry, value);
    }

    public final void swapConsecutiveTwoTables() {
        AtomicDoubleArray tmp = tables[tablePos];
        tables[tablePos] = tables[tablePos + 1];
        tables[tablePos + 1] = tmp;
    }
}
