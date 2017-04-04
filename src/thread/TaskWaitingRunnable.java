package thread;

import task.Task;

import java.util.concurrent.LinkedBlockingQueue;


public class TaskWaitingRunnable implements Runnable {
    LinkedBlockingQueue<Task> taskQueue;
    static boolean isfinish = false;

    public TaskWaitingRunnable(LinkedBlockingQueue<Task> taskQueue) {
        this.taskQueue = taskQueue;
    }

    @Override
    public void run() {
        while (taskQueue.size() != 0 || !isfinish) {
            Task task = null;
            try {
                task = taskQueue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            task.run();
        }
    }
}
