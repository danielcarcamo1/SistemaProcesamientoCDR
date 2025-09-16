import java.time.LocalDateTime;
//clse que consume registros CDR de la cola y los procesa
public class CDRConsumer implements Runnable {
    private final CDRQueue queue;
    private final String consumerName;
    private final CDRDatabase database;
    private final CDRProcessor processor;
    private int recordsConsumed;
    private int totalMinutesProcessed;
    private final LocalDateTime startTime;
    private volatile boolean running = true;

    //atributos
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

    //metodo run
    @Override
    public void run() {
        processor.updateConsumerStatus(consumerName, "Started", recordsConsumed, totalMinutesProcessed, startTime);

        try {
            //bucle principal de consumo
            while (running && !Thread.currentThread().isInterrupted()) {
                CDR cdr = queue.take();
                processCDR(cdr);
                recordsConsumed++;
                processor.updateConsumerStatus(consumerName, "Running", recordsConsumed, totalMinutesProcessed, startTime);
                Thread.sleep(100); //tiempo de sumulacion de procesamiento
            }
            //actualizacion de estado al finalizar normalmente
            processor.updateConsumerStatus(consumerName, "Completed", recordsConsumed, totalMinutesProcessed, startTime);
        } catch (InterruptedException e) {
            //manejo de interrupcion
            processor.updateConsumerStatus(consumerName, "Interrupted", recordsConsumed, totalMinutesProcessed, startTime);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            //menjo de otros errores
            processor.updateConsumerStatus(consumerName, "Error: " + e.getMessage(), recordsConsumed, totalMinutesProcessed, startTime);
        }
    }

    //metodo de procesamiento
    private void processCDR(CDR cdr) throws Exception {
        //insecion en la BD
        database.insertCDR(cdr);
        //actualizacion de resumen de cuenta
        database.updateAccountSummary(cdr.getAccountNumber(), cdr.getDurationMinutes(), cdr.getCost(), cdr.getCallType());
        //actualizacion de contador interno
        totalMinutesProcessed += cdr.getDurationMinutes();
    }

    //metodos de control

    //detiene  la ejecucion del consumifor
    public void stop() {
        this.running = false;
    }

    //metodos getter
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