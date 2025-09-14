import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicDouble;

public class CDRConsumer implements Runnable {
    private final CDRQueue queue;
    private final String consumerName;
    private final ConcurrentHashMap<String, AtomicInteger> totalMinutes;
    private final ConcurrentHashMap<String, AtomicDouble> totalCosts;

    public CDRConsumer(CDRQueue queue, String name,
                       ConcurrentHashMap<String, AtomicInteger> totalMinutes,
                       ConcurrentHashMap<String, AtomicDouble> totalCosts) {
        this.queue = queue;
        this.consumerName = name;
        this.totalMinutes = totalMinutes;
        this.totalCosts = totalCosts;
    }

    @Override
    public void run() {
        try {
            int recordsProcessed = 0;
            while (true) {
                CDR cdr = queue.take();

                // Process CDR
                processCDR(cdr);
                recordsProcessed++;

                System.out.println(consumerName + " consumed: " + cdr.getAccountNumber());
                Thread.sleep(100); // Simulate processing time
            }
        } catch (InterruptedException e) {
            System.out.println(consumerName + " finished. Processed: " + recordsProcessed);
            Thread.currentThread().interrupt();
        }
    }

    private void processCDR(CDR cdr) {
        String account = cdr.getAccountNumber();

        // Update total minutes
        totalMinutes.putIfAbsent(account, new AtomicInteger(0));
        totalMinutes.get(account).addAndGet(cdr.getDurationMinutes());

        // Update total cost
        totalCosts.putIfAbsent(account, new AtomicDouble(0.0));
        totalCosts.get(account).addAndGet(cdr.getCost());
    }
}
