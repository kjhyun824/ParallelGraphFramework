package algorithm.pagerank.personalized;

import graph.Graph;
import graph.GraphAlgorithmInterface;
import graph.Node;
import graph.sharedData.PersonalPageRankSharedData;

public class PersonalPageRankExecutor implements GraphAlgorithmInterface
{
    Graph<PersonalPageRankSharedData> graph;
    PersonalPageRankSharedData sharedDataObject;
    Node srcNode;

    final int beginRange;
    final int endRange;
    final double dampingFactor;

    PersonalPageRankExecutor(int beginRange, int endRange, Graph<PersonalPageRankSharedData> graph, double dampingFactor) {
        this.graph = graph;
        this.beginRange = beginRange;
        this.endRange = endRange;
        this.dampingFactor = dampingFactor;
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
            double curPageRank = sharedDataObject.getVertexValue(srcNode.getInDegree(), i);
            if (curPageRank == 0) {
                continue;
            }
            double scatterPageRank = dampingFactor * (curPageRank / (double) neighborListSize);


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
