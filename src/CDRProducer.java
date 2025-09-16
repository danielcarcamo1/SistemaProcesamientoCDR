import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CDRProducer implements Runnable {
    private final String filePath;
    private final CDRQueue queue;
    private final String producerName;
    private final CDRProcessor processor;
    private int recordsProduced;
    private final LocalDateTime startTime;

    public CDRProducer(String filePath, CDRQueue queue, String name, CDRProcessor processor) {
        this.filePath = filePath;
        this.queue = queue;
        this.producerName = name;
        this.processor = processor;
        this.recordsProduced = 0;
        this.startTime = LocalDateTime.now();
    }

    //metodo run
    @Override
    public void run() {
        processor.updateProducerStatus(producerName, "Started", recordsProduced, startTime);

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            reader.readLine(); //saltar encabezado (skip header)

            while ((line = reader.readLine()) != null) {
                CDR cdr = parseCDR(line);
                if (cdr != null) {
                    queue.put(cdr);
                    recordsProduced++;
                    processor.updateProducerStatus(producerName, "Running", recordsProduced, startTime);
                }
                //tiempo de ssimulacio del procesamiento
                Thread.sleep(50);
            }

            processor.updateProducerStatus(producerName, "Completed", recordsProduced, startTime);
        } catch (Exception e) {
            processor.updateProducerStatus(producerName, "Error: " + e.getMessage(), recordsProduced, startTime);
        }
    }

    //metodo de parseo convierte csv en un objeto CDR
    private CDR parseCDR(String line) {
        String[] parts = line.split(",");
        if (parts.length < 7) return null;

        try {
            return new CDR(
                    parts[0].trim(),
                    parts[1].trim(),
                    parts[2].trim(),
                    parts[3].trim(),
                    Integer.parseInt(parts[4].trim()),
                    Double.parseDouble(parts[5].trim()),
                    parts[6].trim()
            );
        } catch (NumberFormatException e) {
            return null;
        }
    }

    //metodos getter
    public int getRecordsProduced() {
        return recordsProduced;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }
}