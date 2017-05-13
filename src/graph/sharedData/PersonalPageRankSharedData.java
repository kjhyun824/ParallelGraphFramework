package graph.sharedData;

import atomic.AtomicDoubleArray;

import java.util.function.DoubleBinaryOperator;

public class PersonalPageRankSharedData
{
    public static DoubleBinaryOperator updateFunction;

    public static void setUpdateFunction(DoubleBinaryOperator function) {
        updateFunction = function;
    }

    AtomicDoubleArray[] tables;
    byte[] seedCheckArray;

    final int nodeCapacity;
    final int asyncThreshold;
    int tablePos = 0;

    public PersonalPageRankSharedData(int nodeCapacity, int asyncThreshold) {
        this.nodeCapacity = nodeCapacity;
        this.asyncThreshold = asyncThreshold;
    }

    public final void initializeTable() {
        seedCheckArray = new byte[nodeCapacity];
        tables = new AtomicDoubleArray[2];
        for (int i = 0; i < tables.length; i++) {
            tables[i] = new AtomicDoubleArray(nodeCapacity);
        }

        for (int i = 0; i < tables.length; i++) {
            for (int j = 0; j < nodeCapacity; j++) {
                tables[i].set(j,0);
            }
        }
    }

    public final void initializedCallback() {
        swapConsecutiveTwoTables();
    }

    public void setSeedNode(int pos) {
        seedCheckArray[pos] = 1;
    }

    public boolean isNodeSeed(int pos) {
        return seedCheckArray[pos] == 1;
    }

    public final void setVertexValue(int degree, int entry, double value) {
        if (degree < asyncThreshold) {
            tables[tablePos].asyncSet(entry, value);
        }
        else {
            tables[tablePos].set(entry, value);
        }
    }

    public final void setNextVertexValue(int degree, int entry, double value) {
        if (degree < asyncThreshold) {
            tables[tablePos + 1].asyncSet(entry, value);
        }
        else {
            tables[tablePos + 1].set(entry, value);
        }
    }

    public final double getVertexValue(int degree, int entry) {
        if (degree < asyncThreshold) {
            return tables[tablePos].asyncGet(entry);
        }
        else {
            return tables[tablePos].get(entry);
        }
    }

    public final void update(int degree, int entry, double value) {
        update(tablePos, degree, entry, value);
    }

    public final void update(int pos, int degree, int entry, double value) {
        if (degree < asyncThreshold) { // TODO : think about multiple ranges in a single sharedData
            tables[pos].asyncGetAndAccumulate(entry, value, updateFunction);
        }
        else {
            tables[pos].getAndAccumulate(entry, value, updateFunction);
        }
    }

    public final void updateNextTable(int degree, int entry, double value) {
        update(tablePos + 1, degree, entry, value);
    }

    public final void swapConsecutiveTwoTables() {
        AtomicDoubleArray tmp = tables[tablePos];
        tables[tablePos] = tables[tablePos + 1];
        tables[tablePos + 1] = tmp;
    }

    public void reset() {
        initializeTable();
    }
}
