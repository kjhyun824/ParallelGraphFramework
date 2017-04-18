package thread;

import task.Task;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SSSPTaskWaitingRunnable implements Runnable
{
    LinkedBlockingQueue<Task> taskQueue;
    ReentrantLock lock;
    Condition condition;

    public SSSPTaskWaitingRunnable(LinkedBlockingQueue<Task> taskQueue, ReentrantLock lock, Condition condition) {
        this.taskQueue = taskQueue;
        this.lock = lock;
        this.condition = condition;
    }

    @Override
    public void run() {
        while (true) {
            lock.lock();
            try {
                condition.await();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                lock.unlock();
            }
            while (taskQueue.size() != 0) {
                Task task = null;
                try {
                    task = taskQueue.take();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                task.run();
            }
        }
    }
}
