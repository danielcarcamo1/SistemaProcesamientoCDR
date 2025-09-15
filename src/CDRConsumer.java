import java.time.LocalDateTime;

public class CDRConsumer implements Runnable {
    private final CDRQueue queue;
    private final String consumerName;
    private final CDRDatabase database;
    private final CDRProcessor processor;
    private int recordsConsumed;
    private int totalMinutesProcessed;
    private final LocalDateTime startTime;
    private volatile boolean running = true;

    public CDRConsumer(CDRQueue queue, String name, CDRDatabase database, CDRProcessor processor) {
        this.queue = queue;
        this.consumerName = name;
        this.database = database;
        this.processor = processor;
        this.recordsConsumed = 0;
        this.totalMinutesProcessed = 0;
        this.startTime = LocalDateTime.now();
        this.running = true;
    }

    @Override
    public void run() {
        processor.updateConsumerStatus(consumerName, "Started", recordsConsumed, totalMinutesProcessed, startTime);

        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                CDR cdr = queue.take();
                processCDR(cdr);
                recordsConsumed++;
                processor.updateConsumerStatus(consumerName, "Running", recordsConsumed, totalMinutesProcessed, startTime);
                Thread.sleep(100);
            }
            processor.updateConsumerStatus(consumerName, "Completed", recordsConsumed, totalMinutesProcessed, startTime);
        } catch (InterruptedException e) {
            processor.updateConsumerStatus(consumerName, "Interrupted", recordsConsumed, totalMinutesProcessed, startTime);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            processor.updateConsumerStatus(consumerName, "Error: " + e.getMessage(), recordsConsumed, totalMinutesProcessed, startTime);
        }
    }

    private void processCDR(CDR cdr) throws Exception {
        database.insertCDR(cdr);
        database.updateAccountSummary(cdr.getAccountNumber(), cdr.getDurationMinutes(), cdr.getCost(), cdr.getCallType());
        totalMinutesProcessed += cdr.getDurationMinutes();
    }

    public void stop() {
        this.running = false;
    }

    public int getRecordsConsumed() {
        return recordsConsumed;
    }

    public int getTotalMinutesProcessed() {
        return totalMinutesProcessed;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }
}