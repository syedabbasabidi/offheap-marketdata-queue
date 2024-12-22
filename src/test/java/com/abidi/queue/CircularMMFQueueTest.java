package com.abidi.queue;

import com.abidi.marketdata.model.MarketData;
import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.util.ByteUtils;
import com.abidi.util.ChecksumUtil;
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
    public static final String QUOTE_EXPIRY_DATE = DATE_TIME_FORMATTER.format(LocalDateTime.of(2024, 11, 5, 13, 22, 43));
    public static final String BRC = "BRC";
    public static final int SIDE = 1;
    public static final String SEC_ID = "GB00BJLR0J16";
    public static final double PRICE = 103.12;
    public static final int ID = 1122;
    public static final boolean IS_FIRM = true;
    public static final byte PRICE_TYPE = 1;
    private CircularMMFQueue queue;
    private ByteUtils byteUtils;
    private ChecksumUtil checksumUtil;


    @BeforeEach
    public void setup() throws IOException {
        byteUtils = new ByteUtils();
        checksumUtil = new ChecksumUtil();
        MarketData md = new MarketData(byteUtils, checksumUtil);
        queue = getInstance(md.size(), SIZE, "/tmp");
    }

    @AfterEach
    public void destroy() {
        queue.cleanup();
    }

    @Test
    @DisplayName("Queue created, ensure it's properties (empty, size, messages written and read)")
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
        queue.add(md.getData());

        assertEquals(1, queue.messagesWritten());
        assertEquals(0, queue.messagesRead());
        assertFalse(queue.isEmpty());
        assertFalse(queue.isFull());

        queue.closeQueue();
        queue = getInstance(md.size(), SIZE, "/tmp");

        assertEquals(1, queue.getQueueSize());
        MarketDataCons mdConsumer = new MarketDataCons(byteUtils);
        mdConsumer.setData(queue.get());

        assertMarketDataMatches(mdConsumer, md);


        assertEquals(0, queue.getQueueSize());
        assertEquals(1, queue.messagesWritten());
        assertEquals(1, queue.messagesRead());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
    }

    private void assertMarketDataMatches(MarketDataCons mdConsumer, MarketData md) {
        assertEquals(String.valueOf(mdConsumer.getSec()), SEC_ID);
        assertEquals(mdConsumer.getId(), ID);
        assertEquals(mdConsumer.getPrice(), PRICE, 0.00000000001d);
        assertEquals(String.valueOf(mdConsumer.getBroker()), BRC);
        assertEquals(mdConsumer.getSide(), SIDE);
        assertEquals(mdConsumer.getPriceType(), PRICE_TYPE);
        assertEquals(String.valueOf(mdConsumer.getValidUntil()), QUOTE_EXPIRY_DATE);
        assertEquals(mdConsumer.getChecksum(), checksumUtil.checksum(md.getData()));
    }

    @Test
    @DisplayName("Producer fills queue with half the size of queue and consumer consumes it")
    public void test3() {

        MarketData md = getMarketData();
        rangeClosed(1, SIZE / 2).forEach(j -> queue.add(md.getData()));

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
        rangeClosed(1, SIZE).forEach(j -> queue.add(md.getData()));

        assertEquals(queue.getQueueSize(), SIZE);
        assertFalse(queue.isEmpty());
        assertTrue(queue.isFull());

        MarketDataCons mdConsumer = new MarketDataCons(byteUtils);
        rangeClosed(1, SIZE).forEach(j -> {
            mdConsumer.setData(queue.get());
            assertMarketDataMatches(mdConsumer, md);
        });

        assertEquals(queue.getQueueSize(), 0);
        assertEquals(SIZE, queue.messagesWritten());
        assertEquals(SIZE, queue.messagesRead());
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());

        rangeClosed(1, SIZE).forEach(j -> queue.add(md.getData()));

        assertEquals(queue.getQueueSize(), SIZE);
        assertFalse(queue.isEmpty());
        assertTrue(queue.isFull());
    }

    private MarketData getMarketData() {
        MarketData md = new MarketData(byteUtils, checksumUtil);
        md.set(SEC_ID, PRICE, SIDE, IS_FIRM, PRICE_TYPE, BRC, QUOTE_EXPIRY_DATE, ID);
        return md;
    }
}