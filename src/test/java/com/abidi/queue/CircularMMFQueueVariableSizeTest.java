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
    private static final int MAX = 100;
    private static final int CAPACITY = 1024;

    private CircularMMFQueue queue;

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.cleanup();
        }
    }

    private CircularMMFQueue open() throws IOException {
        return new CircularMMFQueueVariableSize(MAX, CAPACITY, PATH);
    }

    private CircularMMFQueue fresh() throws IOException {
        queue = open();
        queue.reset();
        return queue;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    @Test
    void messagesKeepTheirOwnLength() throws IOException {
        fresh();
        assertTrue(queue.add(bytes("a")));
        assertTrue(queue.add(bytes("three")));
        assertTrue(queue.add(new byte[MAX]));

        assertArrayEquals(bytes("a"), queue.get());
        int[] seen = new int[2];
        assertTrue(queue.readWithAck((buffer, offset, length) -> {
            seen[0] = length;
            seen[1] = buffer.get(offset);
        }));
        queue.ack();
        assertArrayEquals(new int[]{5, 't'}, seen);
        assertEquals(MAX, queue.get().length);
        assertNull(queue.get());
        assertEquals(3, queue.messagesWritten());
        assertEquals(3, queue.messagesRead());
    }

    @Test
    void wrapsWithPaddingAndRefusesWhenFullUntilRead() throws IOException {
        fresh();
        Random random = new Random(42);
        ArrayDeque<byte[]> pending = new ArrayDeque<>();
        long written = 0;
        long read = 0;
        for (int i = 0; i < 20_000; i++) {
            byte[] msg = new byte[1 + random.nextInt(MAX)];
            random.nextBytes(msg);
            if (queue.add(msg)) {
                pending.add(msg);
                written++;
            } else {
                assertTrue(queue.isFull());
                assertFalse(pending.isEmpty(), "refused an empty queue");
            }
            if (random.nextInt(3) == 0 || !pending.isEmpty() && pending.size() > 8) {
                byte[] got = queue.get();
                if (got != null) {
                    assertArrayEquals(pending.poll(), got);
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
        fresh();
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

        queue = open();
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
        fresh();
        int count = 1_000_000;
        Thread producer = new Thread(() -> {
            try (CircularMMFQueue writer = open()) {
                byte[] msg = new byte[MAX];
                for (int i = 0; i < count; i++) {
                    int length = 2 + i % (MAX - 1);
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
        int[] next = {0};
        try (CircularMMFQueue reader = open()) {
            while (next[0] < count) {
                if (reader.readWithAck((buffer, offset, length) -> {
                    int i = next[0];
                    assertEquals(2 + i % (MAX - 1), length);
                    assertEquals((byte) i, buffer.get(offset));
                    assertEquals((byte) (i >> 8), buffer.get(offset + length - 1));
                })) {
                    reader.ack();
                    next[0]++;
                }
            }
            assertEquals(count, reader.messagesRead());
        }
        producer.join();
        assertEquals(count, queue.messagesWritten());
    }

    @Test
    void rejectsBadSizesAndMismatchedReopen() throws IOException {
        fresh();
        assertThrows(IllegalArgumentException.class, () -> queue.add(new byte[MAX + 1]));
        assertThrows(IllegalArgumentException.class, () -> queue.add(new byte[0]));
        assertThrows(IllegalStateException.class, () -> new CircularMMFQueueVariableSize(MAX, CAPACITY * 2, PATH));
        assertThrows(IllegalArgumentException.class, () -> new CircularMMFQueueVariableSize(MAX, 1000, PATH + "-x"));
        assertThrows(IllegalArgumentException.class, () -> new CircularMMFQueueVariableSize(CAPACITY / 2, CAPACITY, PATH + "-x"));
    }
}
