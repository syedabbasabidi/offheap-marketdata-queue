package com.abidi.queue;

import com.abidi.marketdata.model.MarketData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.abidi.queue.CircularMMFQueue.getInstance;

public class CircularMMFQueueTest { 

    public static final int SIZE = 10;
    private CircularMMFQueue queue;

    @BeforeEach
    public void setup() throws IOException {
        MarketData md = new MarketData();
        queue = getInstance(md.size(), "/tmp");
        queue.cleanup();
    }

    @Test
    public void checkQueueSize() {

        MarketData md = new MarketData();
        md.set("GB00BJLR0J16", 0d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", 0);
        rangeClosed(1, SIZE).forEach(j -> {
            md.setPrice(j);
            md.setId(j);
            queue.add(md.getData());
        });
        assertEquals(queue.getQueueSize(), SIZE);
        rangeClosed(1, SIZE).forEach(j -> queue.get());
        assertEquals(queue.getQueueSize(), 0);
    }

    @Test
    public void checkQueueSizeAfterConsumerHasReadHalfOfTheQueue() {

        MarketData md = new MarketData();
        md.set("GB00BJLR0J16", 0d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", 0);
        rangeClosed(1, SIZE).forEach(j -> {
            md.setPrice(j);
            md.setId(j);
            queue.add(md.getData());
        });
        assertEquals(queue.getQueueSize(), SIZE);
        rangeClosed(1, 5).forEach(j -> queue.get());
        assertEquals(queue.getQueueSize(), 5);

        rangeClosed(1, SIZE).forEach(j -> {
            md.setPrice(j);
            queue.add(md.getData());
        });

        assertEquals(queue.getQueueSize(), SIZE + 5);

    }
}