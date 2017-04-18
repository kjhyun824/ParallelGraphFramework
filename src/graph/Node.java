package graph;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;

public class Node extends TIntArrayList {
    TDoubleArrayList weights = null;

    int inDegree;
    int outDegree;

    Node() {
        weights = new TDoubleArrayList(10);
    }

    public boolean addNeighborId(int neighborNodeId) {
        int pos = binarySearch(neighborNodeId);
        if (pos >= 0) {
            return false;
        } else {
            pos = -(pos + 1);
            insert(pos, neighborNodeId);
            return true;
        }
    }

    public boolean addNeighborId(int neighborNodeId, double weight) {
        int pos = binarySearch(neighborNodeId);
        if (pos >= 0) {
            return false;
        } else {
            pos = -(pos + 1);
            insert(pos, neighborNodeId);
            weights.insert(pos, weight);
            return true;
        }
    }


    public int getNeighbor(int neighborNodeIdx) {
        return getQuick(neighborNodeIdx);
    }

    public double getWeight(int neighborNodeId) {
        int pos = binarySearch(neighborNodeId);
        return weights.get(pos);
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

    /*
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
    */
}
