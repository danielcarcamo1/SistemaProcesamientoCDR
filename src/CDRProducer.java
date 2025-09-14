public class CDRProducer implements Runnable {
    private final CDRFileReader reader;
    private final CDRQueue queue;
    private final String producerName;

    public CDRProducer(CDRFileReader reader, CDRQueue queue, String name) {
        this.reader = reader;
        this.queue = queue;
        this.producerName = name;
    }

    @Override
    public void run() {
        try {
            int recordsProcessed = 0;
            while (true) {
                CDR cdr = reader.readNextCDR();
                if (cdr == null) break;

                queue.put(cdr);
                recordsProcessed++;

                /// //
            /// //
            }
        } catch (Exception e) {
            System.out.println(producerName + "Error:" + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}