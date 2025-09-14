import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        final String FILE_PATH = "cdr_data.csv";
        final int QUEUE_CAPACITY = 1000;
        final int NUM_PRODUCERS = 3;
        final int NUM_CONSUMERS = 2;

        try {
            // Initialize shared data structures
            CDRFileReader reader = new CDRFileReader(FILE_PATH);
            CDRQueue queue = new CDRQueue(QUEUE_CAPACITY);

            ConcurrentHashMap<String, AtomicInteger> totalMinutes = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, AtomicDouble> totalCosts = new ConcurrentHashMap<>();

            CountDownLatch producersLatch = new CountDownLatch(NUM_PRODUCERS);

            // Create consumers
            ExecutorService consumerExecutor = Executors.newFixedThreadPool(NUM_CONSUMERS);
            for (int i = 0; i < NUM_CONSUMERS; i++) {
                CDRConsumer consumer = new CDRConsumer(queue, "Consumer-" + (i+1),
                        totalMinutes, totalCosts);
                consumerExecutor.execute(consumer);
            }