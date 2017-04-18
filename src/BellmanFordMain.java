import algorithm.sssp.BellmanFordDriver;
import algorithm.sssp.DijkstraDriver;
import graph.Graph;
import graph.GraphUtil;
import graph.partition.SSSPPartition;

public class BellmanFordMain
{
    public static void main(String[] args) {
        String inputFile = args[0];

//        Graph<SSSPPartition> graph = Graph.getInstance(0,true,true);
//        System.out.println("[DEBUG] Graph Loading ...");
//        GraphUtil.load(graph, inputFile);
//        System.out.println("[DEBUG] Graph Complete");
//
//        BellmanFordDriver driver = new BellmanFordDriver(graph, 1);
//
//        System.out.println("[DEBUG] Dijkstra Start");
//        driver.run();
//        System.out.println("[DEBUG] Dijkstra END");
//        System.out.println("[DEBUG] Distance Write ...");
//        driver.printDist();
//        System.out.println("[DEBUG] END");
//        System.exit(1);

        String distance = String.format("%.3f", 0.0004);
        System.out.println(distance);
    }
}