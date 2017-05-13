package algorithm.pagerank.original;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.sharedData.PageRankSharedData;

public class PageRankExecutor implements GraphAlgorithmInterface
{
    Graph<PageRankSharedData> graph;
    PageRankSharedData sharedDataObject;
    Node srcNode;

    int beginRange;
    int endRange;
    double dampingFactor;

    PageRankExecutor(int beginRange, int endRange, Graph<PageRankSharedData> graph, double dampingFactor) {
        this.graph = graph;
        this.dampingFactor = dampingFactor;
        this.beginRange = beginRange;
        this.endRange = endRange;
        sharedDataObject = graph.getSharedDataObject();
    }

    @Override
    public void execute() {
        for (int i = beginRange; i < endRange; i++) {
            srcNode = graph.getNode(i);

            if (srcNode == null) {
                continue;
            }

            int neighborListSize = srcNode.neighborListSize();
            double scatterPageRank = dampingFactor * (sharedDataObject.getVertexValue(srcNode.getInDegree(), i) / (double) neighborListSize);

            for (int j = 0; j < neighborListSize; j++) {
                int destId = srcNode.getNeighbor(j);
                Node dest = graph.getNode(destId);
                sharedDataObject.updateNextTable(dest.getInDegree(), destId, scatterPageRank);
            }
        }
    }

    @Override
    public void reset() {

    }
}
