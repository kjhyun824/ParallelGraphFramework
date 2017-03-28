package task;

import graph.GraphAlgorithmInterface;

public class DoubleTask {
    final int partitionId;
	GraphAlgorithmInterface algorithm;

    public DoubleTask(int partitionId, GraphAlgorithmInterface algorithm) {
		this.partitionId = partitionId;
		this.algorithm = algorithm;
    }

    // For JIT Compiler
    public void reset() {
		algorithm.reset(partitionId);
	}
	
    public void run() {
		algorithm.execute(partitionId);
    }
}


