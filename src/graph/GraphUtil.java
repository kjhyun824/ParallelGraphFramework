package graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GraphUtil {
    public static DirectedGraph load(DirectedGraph graph, String inputFile) {
        Path path = Paths.get(inputFile);
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            readEdgeFromFile(graph, reader);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return graph;
    }

    static void readEdgeFromFile(DirectedGraph graph, BufferedReader reader)
            throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] nodeId = line.trim().split("\t");
            int srcNodeId = Integer.parseInt(nodeId[0]);
            int destNodeId = Integer.parseInt(nodeId[1]);
            graph.addEdge(srcNodeId, destNodeId);
        }
    }

    public static DirectedGraph transposeGraph(DirectedGraph graph) {
        DirectedGraph graphT = new DirectedGraph();
        int maxNodeId = graph.getMaxNodeId();

        for (int i = 0; i <= maxNodeId; i++) {
            Node node = graph.getNode(i);
            if (node != null) {
                int neighborListSize = node.neighborListSize();

                for (int j = 0; j < neighborListSize; j++) {
                    int neighborId = node.getNeighbor(j);
                    graphT.addEdge(neighborId, i);
                }
            }
        }
        return graphT;
    }

    public static void finalizeLoading(DirectedGraph graph) {
        graph.finalizeLoading();
    }


    public static void writeGraph(DirectedGraph graph, String fileName) {
        try (FileWriter fw = new FileWriter(fileName, true);
                BufferedWriter bw = new BufferedWriter(fw); PrintWriter out = new PrintWriter(bw)) {

            int maxNodeId = graph.getMaxNodeId();
            for (int i = 0; i <= maxNodeId; i++) {
                Node node = graph.getNode(i);
                int neighborListSize = node.neighborListSize();
                for (int j = 0; j < neighborListSize; j++) {
                    int neighborId = node.getNeighbor(j);
                    out.println(i + "\t" + neighborId);
                }
            }
        }
        catch (IOException e) {

        }
    }
}

/*

각 vertex별로 각 파티션이 가리키는 개수 각각을 테스트

public static void printPartitionDistribution(DirectedGraph graph, GraphPartition graphPartition) {
        int maxNodeId = graph.getMaxNodeId();
        Node[] partitionAdjListOfNode = new Node[maxNodeId + 1];

        getPartititonAdjListOfNode(partitionAdjListOfNode, graph, graphPartition);



        int numPartitions = graphPartition.getNumPartitions();
        int partitionCapacity = graphPartition.getPartitionCapacity();

        for (int i = 0; i < numPartitions; i++) {
            System.out.print("DoubleNodePartition " + i + " / ");
            int[] partitionAdjListSizes = new int[numPartitions];
            int offset = i * partitionCapacity;
            DoubleNodePartition partition = graphPartition.getPartition(i);

            for (int j = 0; j < partition.getSize(); j++) {
                int partitionAdjListSize = partitionAdjListOfNode[offset + j].inNeighborListSize();
                partitionAdjListSizes[partitionAdjListSize]++;
            }

            for (int j = 0; j < partitionAdjListSizes.length; j++) {
                System.out.print(partitionAdjListSizes[j] + " / ");
            }
            System.out.println();
        }
    }
 */