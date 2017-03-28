package graph;

import gnu.trove.list.array.TIntArrayList;

public class Node extends TIntArrayList {
    TIntArrayList transposeNeighbors = null;
    int inDegree;
    int outDegree;

    public boolean addNeighborId(int neighborNodeId) {
        int pos = binarySearch(neighborNodeId);
        if (pos >= 0) {
            return false;
        }
        else {
            pos = -(pos + 1);
            insert(pos, neighborNodeId);
            return true;
        }
    }

    public int getNeighbor(int neighborNodeId) {
        return getQuick(neighborNodeId);
    }

    public int neighborListSize() {
        return size();
    }

    public void incrementInDegree() {
        inDegree++;
    }

    public void incrementOutDegree() {
        outDegree++;
    }

    public int getInDegree() {
        return inDegree;
    }

    public int getOutDegree() {
        return outDegree;
    }

    public void addReverseEdge(int neighborNodeId) {
        if (transposeNeighbors == null) {
            transposeNeighbors = new TIntArrayList();
        }

        int pos = transposeNeighbors.binarySearch(neighborNodeId);
        if (pos < 0) {
            pos = -(pos + 1);
            transposeNeighbors.insert(pos, neighborNodeId);
        }
    }
}

