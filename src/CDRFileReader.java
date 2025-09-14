import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CDRFileReader implements AutoCloseable {
    private final BufferedReader reader;
    private final String filePath;

    public CDRFileReader(String filePath) throws IOException {
        this.filePath = filePath;
        this.reader = new BufferedReader(new FileReader(filePath));
        // Skip header if exists
        reader.readLine();
    }

    public synchronized CDR readNextCDR() throws IOException {
        String line = reader.readLine();
        if (line == null) return null;

        String[] parts = line.split(",");
        if (parts.length < 6) return null;

        //try
    }
}