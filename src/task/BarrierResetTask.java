package task;

import graph.GraphAlgorithmInterface;

import java.util.concurrent.CyclicBarrier;

public class BarrierResetTask implements GraphAlgorithmInterface
{
    CyclicBarrier barriers;

    public BarrierResetTask(CyclicBarrier barrier) {
        this.barriers = barrier;
    }

    @Override
    public void execute() {
        barriers.reset();
    }

    @Override
    public void reset() {

    }
}
