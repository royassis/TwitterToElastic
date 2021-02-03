import java.util.concurrent.CountDownLatch;

public class TwitterStreamerWithThread implements Runnable {

    public static void main(String[] args) {
        TwitterStreamerWithThread prog = new TwitterStreamerWithThread();
        Thread t1 = new Thread(prog);
        t1.start();
    }

    @Override
    public void run() {
        CountDownLatch latch = new CountDownLatch(1);
        ThreadFunction mythread = new ThreadFunction(latch);

        try {
            mythread.run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
