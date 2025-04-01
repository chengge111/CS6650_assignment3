package cs6650;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Client2 {

    private static final int THREAD_COUNT = 32;
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final AtomicInteger successCounter = new AtomicInteger(0);
    private static final AtomicInteger failedCounter = new AtomicInteger(0);
    private static final int maxRequests = 200000;
    private static final int threads = 200;
    private static final AtomicBoolean trigger = new AtomicBoolean(true);

    public static void main(String[] args) throws InterruptedException {
        Log.createCSV();

        long start = System.currentTimeMillis();
        BlockingQueue<HttpRequest> buffer = new LinkedBlockingQueue<>();
        new Thread(new Producer(buffer, THREAD_COUNT, threads, maxRequests)).start();
        CountDownLatch completed1 = new CountDownLatch(1);
        CountDownLatch completed2 = new CountDownLatch(THREAD_COUNT + threads);

        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(new ConsumerInitial(buffer, httpClient, completed1, completed2, successCounter, failedCounter)).start();
        }
        completed1.await();
        if (trigger.get()) {
            trigger.set(false);
            for (int i = 0; i < threads; i++) {
                new Thread(new Consumer(buffer, httpClient, completed2, successCounter, failedCounter, maxRequests)).start();
            }
        }
        completed2.await();
        long end = System.currentTimeMillis();
        double delta = (end - start) / 1000.0;

        Log.statistics();

        System.out.println("Client2 Finished");
        System.out.println("Successful requests: " + successCounter.get());
        System.out.println("Failed requests: " + failedCounter.get());
        System.out.printf("Throughput: %.2f requests/sec\n", maxRequests / delta);

    }
}
