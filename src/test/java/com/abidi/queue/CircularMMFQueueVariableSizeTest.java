package com.abidi.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class CircularMMFQueueVariableSizeTest {

    private static final String PATH = "/tmp/test-mmf-variable";
    private static final int MAX_MESSAGE_SIZE = 100;
    private static final int CAPACITY = 1024;

    private CircularMMFQueue queue;

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.cleanup();
        }
    }

    private CircularMMFQueue openQueue() throws IOException {
        return new CircularMMFQueueVariableSize(MAX_MESSAGE_SIZE, CAPACITY, PATH);
    }

    private CircularMMFQueue openEmptyQueue() throws IOException {
        queue = openQueue();
        queue.reset();
        return queue;
    }

    private static int lengthOfMessage(int i) {
        return 2 + i % (MAX_MESSAGE_SIZE - 1);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    @Test
    void messagesKeepTheirOwnLength() throws IOException {
        openEmptyQueue();
        assertTrue(queue.add(bytes("a")));
        assertTrue(queue.add(bytes("three")));
        assertTrue(queue.add(new byte[MAX_MESSAGE_SIZE]));

        assertArrayEquals(bytes("a"), queue.get());
        int[] lengthAndFirstByte = new int[2];
        assertTrue(queue.readWithAck((buffer, offset, length) -> {
            lengthAndFirstByte[0] = length;
            lengthAndFirstByte[1] = buffer.get(offset);
        }));
        queue.ack();
        assertArrayEquals(new int[]{5, 't'}, lengthAndFirstByte);
        assertEquals(MAX_MESSAGE_SIZE, queue.get().length);
        assertNull(queue.get());
        assertEquals(3, queue.messagesWritten());
        assertEquals(3, queue.messagesRead());
    }

    @Test
    void wrapsWithPaddingAndRefusesWhenFullUntilRead() throws IOException {
        openEmptyQueue();
        Random random = new Random(42);
        ArrayDeque<byte[]> writtenNotRead = new ArrayDeque<>();
        long written = 0;
        long read = 0;
        for (int i = 0; i < 20_000; i++) {
            byte[] msg = new byte[1 + random.nextInt(MAX_MESSAGE_SIZE)];
            random.nextBytes(msg);
            if (queue.add(msg)) {
                writtenNotRead.add(msg);
                written++;
            } else {
                assertTrue(queue.isFull());
                assertFalse(writtenNotRead.isEmpty(), "refused an empty queue");
            }
            boolean readerTakesATurn = random.nextInt(3) == 0 || writtenNotRead.size() > 8;
            if (readerTakesATurn) {
                byte[] got = queue.get();
                if (got != null) {
                    assertArrayEquals(writtenNotRead.poll(), got);
                    read++;
                }
            }
            assertEquals(written, queue.messagesWritten());
            assertEquals(read, queue.messagesRead());
            assertEquals(written - read, queue.getQueueSize());
        }
        assertTrue(written > 1000);
    }

    @Test
    void reopenResumesCountsAndRedeliversUnackedMessage() throws IOException {
        openEmptyQueue();
        for (int i = 0; i < 30; i++) {
            assertTrue(queue.add(bytes("m" + i)));
            assertNotNull(queue.get());
        }
        queue.add(bytes("acked"));
        queue.add(bytes("unacked"));
        queue.add(bytes("unread"));
        assertArrayEquals(bytes("acked"), queue.getWithAck());
        assertNull(queue.getWithAck(), "next message must wait for ack");
        queue.ack();
        assertArrayEquals(bytes("unacked"), queue.getWithAck());
        queue.close();

        queue = openQueue();
        assertEquals(33, queue.messagesWritten());
        assertEquals(31, queue.messagesRead());
        assertArrayEquals(bytes("unacked"), queue.get());
        assertTrue(queue.add(bytes("after-restart")));
        assertArrayEquals(bytes("unread"), queue.get());
        assertArrayEquals(bytes("after-restart"), queue.get());
        assertEquals(34, queue.messagesRead());
    }

    @Test
    void producerAndConsumerOnSeparateThreadsAndMappings() throws Exception {
        openEmptyQueue();
        int messageCount = 1_000_000;
        Thread producer = new Thread(() -> {
            try (CircularMMFQueue writer = openQueue()) {
                byte[] msg = new byte[MAX_MESSAGE_SIZE];
                for (int i = 0; i < messageCount; i++) {
                    int length = lengthOfMessage(i);
                    msg[0] = (byte) i;
                    msg[length - 1] = (byte) (i >> 8);
                    while (!writer.add(msg, 0, length)) {
                        Thread.onSpinWait();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        producer.start();
        int[] messagesConsumed = {0};
        try (CircularMMFQueue reader = openQueue()) {
            while (messagesConsumed[0] < messageCount) {
                if (reader.readWithAck((buffer, offset, length) -> {
                    int i = messagesConsumed[0];
                    assertEquals(lengthOfMessage(i), length);
                    assertEquals((byte) i, buffer.get(offset));
                    assertEquals((byte) (i >> 8), buffer.get(offset + length - 1));
                })) {
                    reader.ack();
                    messagesConsumed[0]++;
                }
            }
            assertEquals(messageCount, reader.messagesRead());
        }
        producer.join();
        assertEquals(messageCount, queue.messagesWritten());
    }

    @Test
    void rejectsBadSizesAndMismatchedReopen() throws IOException {
        openEmptyQueue();
        assertThrows(IllegalArgumentException.class, () -> queue.add(new byte[MAX_MESSAGE_SIZE + 1]));
        assertThrows(IllegalArgumentException.class, () -> queue.add(new byte[0]));
        assertThrows(IllegalStateException.class, () -> new CircularMMFQueueVariableSize(MAX_MESSAGE_SIZE, CAPACITY * 2, PATH));
        assertThrows(IllegalArgumentException.class, () -> new CircularMMFQueueVariableSize(MAX_MESSAGE_SIZE, 1000, PATH + "-x"));
        assertThrows(IllegalArgumentException.class, () -> new CircularMMFQueueVariableSize(CAPACITY / 2, CAPACITY, PATH + "-x"));
    }
}
