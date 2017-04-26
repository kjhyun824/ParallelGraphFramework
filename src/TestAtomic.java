import atomic.AtomicIntegerArray;

/**
 * Created by JungHyun on 2017-04-26.
 */
public class TestAtomic {
    public static void main(String[] args) {
        int num = 500000000;
        AtomicIntegerArray arr = new AtomicIntegerArray(num);

        for(int j = 0; j < 10; j++) {
            for (int i = 0; i < num; i++) {
                arr.compareAndSet(i,0,0);
            }
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            arr.compareAndSet(i,0,i);
        }
        long end = System.currentTimeMillis();
        System.out.println("[DEBUG] Atomic : " + (end-start));

        arr = new AtomicIntegerArray(num);

        for(int j = 0; j < 10; j++) {
            for (int i = 0; i < num; i++) {
                arr.asyncSet(i, i);
            }
        }
        start = System.currentTimeMillis();
        for (int i = 0; i < num; i++) {
            arr.asyncSet(i,i);
        }
        end = System.currentTimeMillis();
        System.out.println("[DEBUG] Async : " + (end-start));
    }
}
