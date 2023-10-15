package com.abidi.queue;

import com.abidi.marketdata.model.MarketData;
import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.util.ByteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.abidi.queue.CircularMMFQueue.getInstance;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.*;

public class CircularMMFQueueTest {

    public static final int SIZE = 10;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd:HH:mm:ss");
    public static final String BRC = "BRC";
    public static final int SIDE = 1;
    public static final String SEC_ID = "GB00BJLR0J16";
    private CircularMMFQueue queue;
    private ByteUtils byteUtils;
    private MarketDataCons mdConsumer;

    @BeforeEach
    public void setup() throws IOException {
        byteUtils = new ByteUtils();
        MarketData md = new MarketData(byteUtils);
        mdConsumer = new MarketDataCons(byteUtils);
        queue = getInstance(md.size(), SIZE, "/tmp");
    }

    @AfterEach
    public void destroy() {
        queue.cleanup();
    }

    @Test
    @DisplayName("Queue created, ensure it's empty")
    public void test0() {
        assertEquals(0, queue.getQueueSize());
        assertEquals(0, queue.messagesWritten());
        assertEquals(0, queue.messagesRead());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
    }


    @Test
    @DisplayName("Add one msg, close down the queue, open it for consumption and consume the persisted msg")
    public void test1() throws IOException {

        MarketData md = getMarketData();
        md.setPrice(103.12);
        md.setId(1123);
        queue.add(md.getData());
        assertEquals(1, queue.messagesWritten());
        assertEquals(0, queue.messagesRead());
        assertFalse(queue.isEmpty());
        assertFalse(queue.isFull());

        queue.closeQueue();
        queue = getInstance(md.size(), SIZE, "/tmp");

        assertEquals(1, queue.getQueueSize());
        mdConsumer.setData(queue.get());
        assertEquals(String.valueOf(mdConsumer.getSec()), SEC_ID);
        assertEquals(0, queue.getQueueSize());
        assertEquals(1, queue.messagesWritten());
        assertEquals(1, queue.messagesRead());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
    }

    @Test
    @DisplayName("Add one msg and consume it")
    public void test2() {

        MarketData md = getMarketData();
        md.setPrice(103.12);
        md.setId(1123);
        queue.add(md.getData());
        assertFalse(queue.isEmpty());
        assertFalse(queue.isFull());
        assertEquals(1, queue.getQueueSize());
        mdConsumer.setData(queue.get());
        assertEquals(String.valueOf(mdConsumer.getSec()), SEC_ID);
        assertEquals(0, queue.getQueueSize());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
    }

    @Test
    @DisplayName("Producer fills queue with half the size of queue and consumer consumes it")
    public void test3() {

        MarketData md = getMarketData();
        rangeClosed(1, SIZE / 2).forEach(j -> {
            md.setPrice(j);
            md.setId(j);
            queue.add(md.getData());
        });
        assertFalse(queue.isEmpty());
        assertFalse(queue.isFull());
        assertEquals(queue.getQueueSize(), SIZE / 2);
        rangeClosed(1, SIZE / 2).forEach(j -> queue.get());
        assertEquals(queue.getQueueSize(), 0);
        assertEquals(SIZE / 2, queue.messagesWritten());
        assertEquals(SIZE / 2, queue.messagesRead());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
    }

    @Test
    @DisplayName("Producer fills the queue after consumers consumes all")
    public void test4() {

        MarketData md = getMarketData();
        rangeClosed(1, SIZE).forEach(j -> {
            md.setPrice(j);
            md.setId(j);
            queue.add(md.getData());
        });
        assertEquals(queue.getQueueSize(), SIZE);
        assertFalse(queue.isEmpty());
        assertTrue(queue.isFull());

        rangeClosed(1, SIZE).forEach(j -> queue.get());

        assertEquals(queue.getQueueSize(), 0);
        assertEquals(SIZE, queue.messagesWritten());
        assertEquals(SIZE, queue.messagesRead());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());

        rangeClosed(1, SIZE).forEach(j -> {
            md.setPrice(j);
            queue.add(md.getData());
        });

        assertEquals(queue.getQueueSize(), SIZE);
        assertFalse(queue.isEmpty());
        assertTrue(queue.isFull());
    }

    private MarketData getMarketData() {
        MarketData md = new MarketData(byteUtils);
        md.set(SEC_ID, 0d, SIDE, true, (byte) 1, BRC, DATE_TIME_FORMATTER.format(LocalDateTime.now()), 0);
        return md;
    }
}