package task;

import graph.GraphAlgorithmInterface;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class TaskBarrier implements GraphAlgorithmInterface {
    CyclicBarrier barriers;
    
    public TaskBarrier(int id, CyclicBarrier barrier) {
        this.barriers = barrier;
    }
    
    @Override
    public void execute(int id) {
        try {
            barriers.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void reset(int taskId) {
        
    }
}
