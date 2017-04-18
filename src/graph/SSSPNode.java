package graph;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;

public class SSSPNode extends TIntArrayList {
    //    TIntArrayList transposeNeighbors = null;
    TDoubleArrayList heavyEdges = new TDoubleArrayList();
    TDoubleArrayList lightEdges = new TDoubleArrayList();
    TDoubleArrayList weights = new TDoubleArrayList();

    int inDegree;
    int outDegree;
    static double delta;

    public static void setDelta(double d) {
        delta = d;
    }

    public boolean addNeighborId(int neighborNodeId, double weight) {
        if (weight < delta) {
            int pos = lightEdges.binarySearch(neighborNodeId);
            if (pos >= 0) {
                return false;
            }
            else {
                pos = -(pos + 1);
                lightEdges.insert(pos, neighborNodeId);
                weights.insert(pos, weight);
                return true;
            }
        }
        return false;
    }


    public int getNeighbor(int neighborNodeIdx) {
        return getQuick(neighborNodeIdx);
    }

    public double getWeight(int neighborNodeIdx) {
        return weights.get(neighborNodeIdx);
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

