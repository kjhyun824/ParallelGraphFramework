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
        DirectedGraph graphT = new DirectedGraph(graph.getExpOfPartitionSize());
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

