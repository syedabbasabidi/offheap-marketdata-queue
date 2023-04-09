package queue;

import model.MarketData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static queue.CircularMMFQueue.getInstance;

public class CircularMMFQueueTest {

    private CircularMMFQueue queue;

    @BeforeEach
    public void setup() throws IOException {
        MarketData md = new MarketData();
        queue = getInstance(md.size(), 100, "/tmp");
        queue.cleanup();
    }

    @Test
    public void checkQueueSize() {

        MarketData md = new MarketData();
        md.set("GB00BJLR0J16", 0d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13");
        rangeClosed(1, 100).forEach(j -> {
            md.setPrice(j);
            queue.add(md.getData());
        });
        assertEquals(queue.getQueueSize(), 100);
        rangeClosed(1, 100).forEach(j -> queue.get());
        assertEquals(queue.getQueueSize(), 0);

    }

}