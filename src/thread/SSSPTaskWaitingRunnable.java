package thread;

import algorithm.sssp.SSSPDriver;
import algorithm.sssp.SSSPExecutor;
import task.Task;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SSSPTaskWaitingRunnable implements Runnable
{
    LinkedBlockingQueue<Task> taskQueue;
    ReentrantLock lock;
    Condition condition;
    boolean isHeavy;

    public SSSPTaskWaitingRunnable(LinkedBlockingQueue<Task> taskQueue, ReentrantLock lock, Condition condition) {
        this.taskQueue = taskQueue;
        this.lock = lock;
        this.condition = condition;
        this.isHeavy = false;
    }

    @Override
    public void run() {
        while (true) {
            if(!isHeavy) {
                lock.lock();
                try {
                    condition.await();
                    if (taskQueue.size() == 0)
                        isHeavy = true;
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                finally {
                    lock.unlock();
                }
            }
            while (true) {
                Task task = null;
                try {
                    task = taskQueue.take();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                task.run();

                if(taskQueue.size() == 0) {
                    if(isHeavy) isHeavy = false;
                    break;
                }
            }
        }
    }
}
