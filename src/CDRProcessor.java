import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;


//clase principal del sistema de CDRProcessor
public class CDRProcessor {
    private final ConcurrentHashMap<String, String> producerStatus = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> consumerStatus = new ConcurrentHashMap<>();
    private final CDRDatabase database;
    private ExecutorService producerExecutor;
    private ExecutorService consumerExecutor;
    private List<CDRConsumer> consumers;

    //constructor
    public CDRProcessor(CDRDatabase database) {
        this.database = database;
        this.consumers = new ArrayList<>();
    }

    //inia el procesamiento de CDR con el número especificado de productores y consumidores
    public void startProcessing(String filePath, int numProducers, int numConsumers) {
        CDRQueue queue = new CDRQueue(1000);

        //cracion de pool de hilos
        producerExecutor = Executors.newFixedThreadPool(numProducers);
        consumerExecutor = Executors.newFixedThreadPool(numConsumers);
        consumers = new ArrayList<>();

        //inicializacion de consumidores
        for (int i = 0; i < numConsumers; i++) {
            CDRConsumer consumer = new CDRConsumer(queue, "Consumer-" + (i+1), database, this);
            consumers.add(consumer);
            consumerExecutor.execute(consumer);
        }

        //inicializacion de los productores
        for (int i = 0; i < numProducers; i++) {
            CDRProducer producer = new CDRProducer(filePath, queue, "Producer-" + (i+1), this);
            producerExecutor.execute(producer);
        }
    }

    //metodo de detencion de procesamiento
    public void stopProcessing() {
        System.out.println("Deteniendo procesamiento...");

        //dtener a los productores
        if (producerExecutor != null) {
            producerExecutor.shutdownNow();
        }

        //solicitar detencion de consumidores
        if (consumers != null && !consumers.isEmpty()) {
            for (CDRConsumer consumer : consumers) {
                consumer.stop();
            }
        }

        //manejo de detencion de los consumidores
        if (consumerExecutor != null) {
            consumerExecutor.shutdown();
            try {
                //esperar hasta 5 segundos para finalizacion
                if (!consumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    consumerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                //si el hilo actual es interrumpido, forza finalizacion y restaura estado
                consumerExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    //metodos de actualizacion de estado
    public void updateProducerStatus(String name, String status, int records, LocalDateTime startTime) {
        String formattedTime = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String message = String.format("Start: %s | Records: %d | Status: %s", formattedTime, records, status);
        producerStatus.put(name, message);
    }

    //actualiza el estado de un cosumidor en el mapa de estados
    public void updateConsumerStatus(String name, String status, int records, int minutes, LocalDateTime startTime) {
        String formattedTime = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String message = String.format("Start: %s | Records: %d | Minutes: %d | Status: %s",
                formattedTime, records, minutes, status);
        consumerStatus.put(name, message);
    }

    //metodos de consulta de estado
    //obtiene una copia del mapa de estados de prudctores
    public ConcurrentHashMap<String, String> getProducerStatus() {
        return new ConcurrentHashMap<>(producerStatus);
    }
    //obtiene una copia del mapa de estados de consumidores
    public ConcurrentHashMap<String, String> getConsumerStatus() {
        return new ConcurrentHashMap<>(consumerStatus);
    }

    //metodos de obterncion de resultados
    public List<String> getResults() {
        try {
            return database.getAccountSummary();
        } catch (Exception e) {
            System.out.println("Error getting results from database: " + e.getMessage());
            List<String> errorResult = new ArrayList<>();
            errorResult.add("Error al obtener resultados: " + e.getMessage());
            return errorResult;
        }
    }

    //metodo de verificacion de estado
    public boolean isProcessing() {
        return producerExecutor != null && !producerExecutor.isShutdown() ||
                consumerExecutor != null && !consumerExecutor.isShutdown();
    }
}