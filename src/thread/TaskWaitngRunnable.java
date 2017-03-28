package thread;

import task.DoubleTask;

import java.util.concurrent.LinkedBlockingQueue;


public class TaskWaitngRunnable implements Runnable {
    LinkedBlockingQueue<DoubleTask> taskQueue;
    static boolean isfinish = false;

    public TaskWaitngRunnable(LinkedBlockingQueue<DoubleTask> taskQueue) {
        this.taskQueue = taskQueue;
    }

    @Override
    public void run() {
        while (taskQueue.size() != 0 || !isfinish) {
            DoubleTask task = null;
            try {
                task = taskQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            task.run();
        }
    }
}
