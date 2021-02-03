public class TwitterStreamerWithThread implements Runnable {

    @Override
    public void run() {
        System.out.println("Hello from a thread!");
    }

    public static void main(String[] args) {
        TwitterStreamerWithThread prog = new TwitterStreamerWithThread();
        Thread t1 = new Thread(prog);
        t1.start();

    }
}
