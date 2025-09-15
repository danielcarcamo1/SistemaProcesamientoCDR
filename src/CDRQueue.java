import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class CDRQueue {
    private final BlockingQueue<CDR> queue;

    //constructor
    public CDRQueue(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    //metodo que bloquea si la cola se encuentra llena
    public void put(CDR cdr) throws InterruptedException {
        queue.put(cdr);
    }

    //metodo que bloque si la cola se encuentra vacia
    public CDR take() throws InterruptedException {
        return queue.take();
    }

    //retorna el numero de elementos en la cola
    public int size() {
        return queue.size();
    }

    //retorna si la cola se encuentra vacia
    public boolean isEmpty() {
        return queue.isEmpty();
    }
}