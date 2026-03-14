package com.abidi.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link CircularMMFQueue} covering:
 * - Basic add/get with raw byte arrays
 * - FIFO ordering with distinct messages
 * - Full and empty boundary conditions
 * - getWithAck / ack at-most-once delivery protocol
 * - Circular wrap-around across multiple fill/drain cycles
 * - Persistence of reader and writer state across close/reopen
 * - reset() behaviour
 * - Internal buffer reuse (zero-allocation contract)
 * - Factory methods and different message sizes
 */
public class CircularMMFQueueTest {

    private static final int MSG_SIZE = 64;
    private static final int QUEUE_CAPACITY = 8;
    private static final String QUEUE_PATH = "/tmp/test-mmf-comprehensive";

    private CircularMMFQueue queue;

    @BeforeEach
    void setUp() throws IOException {
        queue = new CircularMMFQueue(MSG_SIZE, QUEUE_CAPACITY, QUEUE_PATH);
        queue.reset();
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            queue.cleanup();
        }
    }

    /**
     * Creates a distinguishable 64-byte message whose first two bytes encode the id.
     */
    private byte[] createMessage(int id) {
        byte[] msg = new byte[MSG_SIZE];
        msg[0] = (byte) (id & 0xFF);
        msg[1] = (byte) ((id >> 8) & 0xFF);
        for (int i = 2; i < MSG_SIZE; i++) {
            msg[i] = (byte) ((id + i) & 0xFF);
        }
        return msg;
    }

    // -----------------------------------------------------------------------
    //  Basic state
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Freshly reset queue is empty with all counters at zero")
    void freshQueueState() {
        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
        assertEquals(0, queue.getQueueSize());
        assertEquals(0, queue.messagesWritten());
        assertEquals(0, queue.messagesRead());
    }

    @Test
    @DisplayName("get() on empty queue returns null")
    void getOnEmptyReturnsNull() {
        assertNull(queue.get());
    }

    @Test
    @DisplayName("getWithAck() on empty queue returns null")
    void getWithAckOnEmptyReturnsNull() {
        assertNull(queue.getWithAck());
    }

    // -----------------------------------------------------------------------
    //  Single message round-trip
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Add single message then verify queue state before consumption")
    void addSingleMessageState() {
        assertTrue(queue.add(createMessage(1)));
        assertFalse(queue.isEmpty());
        assertFalse(queue.isFull());
        assertEquals(1, queue.getQueueSize());
        assertEquals(1, queue.messagesWritten());
        assertEquals(0, queue.messagesRead());
    }

    @Test
    @DisplayName("Add and get single message preserves byte-for-byte data")
    void singleMessageDataIntegrity() {
        byte[] msg = createMessage(42);
        queue.add(msg);

        byte[] result = queue.get();
        assertNotNull(result);
        assertArrayEquals(msg, result);
        assertTrue(queue.isEmpty());
        assertEquals(1, queue.messagesWritten());
        assertEquals(1, queue.messagesRead());
    }

    // -----------------------------------------------------------------------
    //  FIFO ordering
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Multiple distinct messages are dequeued in FIFO order")
    void fifoOrdering() {
        byte[] msg1 = createMessage(10);
        byte[] msg2 = createMessage(20);
        byte[] msg3 = createMessage(30);
        queue.add(msg1);
        queue.add(msg2);
        queue.add(msg3);

        // Must copy – get() returns the same internal buffer each time
        byte[] r1 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r2 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r3 = Arrays.copyOf(queue.get(), MSG_SIZE);

        assertArrayEquals(msg1, r1);
        assertArrayEquals(msg2, r2);
        assertArrayEquals(msg3, r3);
    }

    // -----------------------------------------------------------------------
    //  Full queue boundary
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Queue reports full after adding exactly capacity messages")
    void queueReportsFull() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            assertTrue(queue.add(createMessage(i)), "add should succeed for message " + i);
        }
        assertTrue(queue.isFull());
        assertFalse(queue.isEmpty());
        assertEquals(QUEUE_CAPACITY, queue.getQueueSize());
    }

    @Test
    @DisplayName("add() returns false when queue is full")
    void addReturnsFalseWhenFull() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        assertFalse(queue.add(createMessage(99)));
        // Counter should not have advanced
        assertEquals(QUEUE_CAPACITY, queue.messagesWritten());
    }

    @Test
    @DisplayName("add() succeeds again after consuming one element from a full queue")
    void addSucceedsAfterConsumeFromFull() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        assertTrue(queue.isFull());

        queue.get(); // free one slot
        assertFalse(queue.isFull());
        assertTrue(queue.add(createMessage(99)));
        assertEquals(QUEUE_CAPACITY, queue.getQueueSize());
    }

    // -----------------------------------------------------------------------
    //  Circular wrap-around
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Fill → drain → refill verifies data integrity across wrap boundary")
    void wrapAroundDataIntegrity() {
        // First pass
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            byte[] r = queue.get();
            assertNotNull(r);
            assertEquals((byte) i, r[0], "first-pass message " + i);
        }
        assertTrue(queue.isEmpty());

        // Second pass – indices have wrapped
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            assertTrue(queue.add(createMessage(100 + i)));
        }
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            byte[] r = queue.get();
            assertNotNull(r);
            assertEquals((byte) (100 + i), r[0], "second-pass message " + i);
        }
        assertTrue(queue.isEmpty());
    }

    @Test
    @DisplayName("Multiple complete fill/drain cycles succeed")
    void multipleFullCycles() {
        for (int cycle = 0; cycle < 4; cycle++) {
            for (int i = 0; i < QUEUE_CAPACITY; i++) {
                assertTrue(queue.add(createMessage(cycle * QUEUE_CAPACITY + i)),
                        "cycle " + cycle + " add " + i);
            }
            assertTrue(queue.isFull());

            for (int i = 0; i < QUEUE_CAPACITY; i++) {
                byte[] r = queue.get();
                assertNotNull(r, "cycle " + cycle + " get " + i);
                assertEquals((byte) ((cycle * QUEUE_CAPACITY + i) & 0xFF), r[0]);
            }
            assertTrue(queue.isEmpty());
        }
    }

    @Test
    @DisplayName("Interleaved add and get operations maintain FIFO order")
    void interleavedAddGet() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));
        queue.add(createMessage(3));

        byte[] r1 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r2 = Arrays.copyOf(queue.get(), MSG_SIZE);

        queue.add(createMessage(4));
        queue.add(createMessage(5));
        queue.add(createMessage(6));

        byte[] r3 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r4 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r5 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r6 = Arrays.copyOf(queue.get(), MSG_SIZE);

        assertArrayEquals(createMessage(1), r1);
        assertArrayEquals(createMessage(2), r2);
        assertArrayEquals(createMessage(3), r3);
        assertArrayEquals(createMessage(4), r4);
        assertArrayEquals(createMessage(5), r5);
        assertArrayEquals(createMessage(6), r6);
        assertTrue(queue.isEmpty());
    }

    // -----------------------------------------------------------------------
    //  getWithAck / ack protocol
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("getWithAck reads message but does NOT advance the reader index")
    void getWithAckDoesNotAdvanceReader() {
        queue.add(createMessage(1));

        byte[] result = queue.getWithAck();
        assertNotNull(result);
        assertArrayEquals(createMessage(1), result);

        assertEquals(0, queue.messagesRead(), "reader index must not advance until ack");
        assertEquals(1, queue.getQueueSize());
    }

    @Test
    @DisplayName("ack() after getWithAck advances reader and frees the slot")
    void ackAdvancesReader() {
        queue.add(createMessage(1));

        queue.getWithAck();
        queue.ack();

        assertEquals(1, queue.messagesRead());
        assertEquals(0, queue.getQueueSize());
        assertTrue(queue.isEmpty());
    }

    @Test
    @DisplayName("Second getWithAck returns null while first is un-acked")
    void getWithAckBlocksWhileAwaitingAck() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));

        assertNotNull(queue.getWithAck()); // consume first, pending ack
        assertNull(queue.getWithAck(), "must block until outstanding ack is resolved");
    }

    @Test
    @DisplayName("After acking first message, getWithAck returns next message")
    void sequentialGetWithAckCycles() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));
        queue.add(createMessage(3));

        byte[] r1 = queue.getWithAck();
        assertArrayEquals(createMessage(1), r1);
        queue.ack();

        byte[] r2 = queue.getWithAck();
        assertArrayEquals(createMessage(2), r2);
        queue.ack();

        byte[] r3 = queue.getWithAck();
        assertArrayEquals(createMessage(3), r3);
        queue.ack();

        assertTrue(queue.isEmpty());
        assertEquals(3, queue.messagesRead());
    }

    @Test
    @DisplayName("getWithAck/ack works across full queue capacity")
    void getWithAckFullCapacity() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }

        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            byte[] msg = queue.getWithAck();
            assertNotNull(msg, "getWithAck should return message " + i);
            assertEquals((byte) i, msg[0]);
            queue.ack();
        }
        assertTrue(queue.isEmpty());
        assertEquals(QUEUE_CAPACITY, queue.messagesRead());
    }

    @Test
    @DisplayName("ack() without prior getWithAck is a safe no-op")
    void ackWithoutGetWithAckIsNoOp() {
        queue.add(createMessage(1));

        long readBefore = queue.messagesRead();
        queue.ack();
        assertEquals(readBefore, queue.messagesRead(), "ack without getWithAck should not change state");
    }

    @Test
    @DisplayName("getWithAck on drained queue returns null")
    void getWithAckOnDrainedQueue() {
        queue.add(createMessage(1));
        assertNotNull(queue.getWithAck());
        queue.ack();

        assertNull(queue.getWithAck());
    }

    @Test
    @DisplayName("Can switch from getWithAck/ack pattern to plain get()")
    void mixGetWithAckAndPlainGet() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));
        queue.add(createMessage(3));

        // Consume first via ack protocol
        byte[] r1 = queue.getWithAck();
        assertArrayEquals(createMessage(1), r1);
        queue.ack();

        // Switch to plain get() for the rest
        byte[] r2 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r3 = queue.get();
        assertArrayEquals(createMessage(2), r2);
        assertArrayEquals(createMessage(3), r3);
        assertTrue(queue.isEmpty());
    }

    // -----------------------------------------------------------------------
    //  Persistence across close / reopen
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Writer state persists across close and reopen")
    void writerStatePersists() throws IOException {
        queue.add(createMessage(10));
        queue.add(createMessage(20));

        queue.closeQueue();
        queue = new CircularMMFQueue(MSG_SIZE, QUEUE_CAPACITY, QUEUE_PATH);

        assertEquals(2, queue.getQueueSize());
        assertEquals(2, queue.messagesWritten());
        assertFalse(queue.isEmpty());
    }

    @Test
    @DisplayName("Reader progress persists – reopened queue resumes from last read position")
    void readerProgressPersists() throws IOException {
        queue.add(createMessage(1));
        queue.add(createMessage(2));
        queue.add(createMessage(3));

        queue.get(); // consume first message

        queue.closeQueue();
        queue = new CircularMMFQueue(MSG_SIZE, QUEUE_CAPACITY, QUEUE_PATH);

        assertEquals(2, queue.getQueueSize());
        assertEquals(3, queue.messagesWritten());
        assertEquals(1, queue.messagesRead());

        // Next get should return second message, not the first
        byte[] result = queue.get();
        assertNotNull(result);
        assertArrayEquals(createMessage(2), result);
    }

    @Test
    @DisplayName("Messages survive full close/reopen cycle with data integrity")
    void fullPersistenceRoundTrip() throws IOException {
        byte[] msg1 = createMessage(55);
        byte[] msg2 = createMessage(66);
        queue.add(msg1);
        queue.add(msg2);

        queue.closeQueue();
        queue = new CircularMMFQueue(MSG_SIZE, QUEUE_CAPACITY, QUEUE_PATH);

        byte[] r1 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r2 = queue.get();

        assertArrayEquals(msg1, r1);
        assertArrayEquals(msg2, r2);
        assertTrue(queue.isEmpty());
    }

    // -----------------------------------------------------------------------
    //  reset()
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("reset() clears all messages and resets indices to zero")
    void resetClearsState() {
        for (int i = 0; i < 5; i++) {
            queue.add(createMessage(i));
        }
        queue.get();
        queue.get();

        queue.reset();

        assertTrue(queue.isEmpty());
        assertFalse(queue.isFull());
        assertEquals(0, queue.getQueueSize());
        assertEquals(0, queue.messagesWritten());
        assertEquals(0, queue.messagesRead());
    }

    @Test
    @DisplayName("Queue is fully usable after reset")
    void usableAfterReset() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));
        queue.reset();

        byte[] msg = createMessage(99);
        assertTrue(queue.add(msg));
        byte[] result = queue.get();
        assertNotNull(result);
        assertArrayEquals(msg, result);
        assertTrue(queue.isEmpty());
    }

    @Test
    @DisplayName("reset() during getWithAck pending allows fresh operations")
    void resetClearsPendingAck() {
        queue.add(createMessage(1));
        queue.getWithAck(); // leaves pending ack

        queue.reset();

        // Queue should be usable from scratch
        assertTrue(queue.isEmpty());
        queue.add(createMessage(2));
        assertNotNull(queue.get());
    }

    // -----------------------------------------------------------------------
    //  Zero-allocation: internal buffer reuse
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("get() always returns the same array reference (zero-allocation contract)")
    void getReturnsSameArrayReference() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));

        byte[] first = queue.get();
        byte[] second = queue.get();

        assertSame(first, second, "get() must return the same internal buffer");
        // The second call overwrites first's data
        assertArrayEquals(createMessage(2), second);
    }

    @Test
    @DisplayName("getWithAck() also returns the same internal buffer")
    void getWithAckReturnsSameArrayReference() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));

        byte[] first = queue.getWithAck();
        queue.ack();
        byte[] second = queue.getWithAck();

        assertSame(first, second, "getWithAck() must reuse the same internal buffer");
    }

    // -----------------------------------------------------------------------
    //  Counter / size tracking
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("getQueueSize tracks pending messages correctly through add/get cycles")
    void queueSizeTracking() {
        assertEquals(0, queue.getQueueSize());
        queue.add(createMessage(1));
        assertEquals(1, queue.getQueueSize());
        queue.add(createMessage(2));
        assertEquals(2, queue.getQueueSize());
        queue.get();
        assertEquals(1, queue.getQueueSize());
        queue.get();
        assertEquals(0, queue.getQueueSize());
    }

    @Test
    @DisplayName("messagesWritten and messagesRead are monotonically increasing across wraps")
    void monotonicCounters() {
        // First cycle
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        assertEquals(QUEUE_CAPACITY, queue.messagesWritten());

        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.get();
        }
        assertEquals(QUEUE_CAPACITY, queue.messagesRead());

        // Second cycle – counters keep going up, not reset to 0
        for (int i = 0; i < 3; i++) {
            queue.add(createMessage(i));
        }
        assertEquals(QUEUE_CAPACITY + 3, queue.messagesWritten());
        assertEquals(QUEUE_CAPACITY, queue.messagesRead());
        assertEquals(3, queue.getQueueSize());
    }

    // -----------------------------------------------------------------------
    //  Factory methods
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("getInstance(msgSize, path) creates a queue with DEFAULT_SIZE capacity")
    void getInstanceDefaultSize() throws IOException {
        String path = "/tmp/test-mmf-default-instance";
        CircularMMFQueue q = CircularMMFQueue.getInstance(MSG_SIZE, path);
        try {
            assertNotNull(q);
            assertTrue(q.isEmpty());
        } finally {
            q.cleanup();
        }
    }

    @Test
    @DisplayName("getInstance(msgSize, queueSize, path) creates a queue with custom capacity")
    void getInstanceCustomSize() throws IOException {
        String path = "/tmp/test-mmf-custom-instance";
        int customSize = 16;
        CircularMMFQueue q = CircularMMFQueue.getInstance(MSG_SIZE, customSize, path);
        try {
            assertNotNull(q);
            assertTrue(q.isEmpty());
            // Fill to custom capacity
            for (int i = 0; i < customSize; i++) {
                assertTrue(q.add(createMessage(i)));
            }
            assertTrue(q.isFull());
        } finally {
            q.cleanup();
        }
    }

    // -----------------------------------------------------------------------
    //  Different message sizes
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Queue operates correctly with a smaller message size")
    void smallerMessageSize() throws IOException {
        int smallSize = 16;
        String path = "/tmp/test-mmf-small-msg";
        CircularMMFQueue q = new CircularMMFQueue(smallSize, QUEUE_CAPACITY, path);
        try {
            q.reset();
            byte[] msg = new byte[smallSize];
            Arrays.fill(msg, (byte) 0xAB);

            assertTrue(q.add(msg));
            byte[] result = q.get();
            assertNotNull(result);
            assertArrayEquals(msg, result);
        } finally {
            q.cleanup();
        }
    }

    @Test
    @DisplayName("Queue operates correctly with a larger message size")
    void largerMessageSize() throws IOException {
        int largeSize = 256;
        String path = "/tmp/test-mmf-large-msg";
        CircularMMFQueue q = new CircularMMFQueue(largeSize, QUEUE_CAPACITY, path);
        try {
            q.reset();
            byte[] msg = new byte[largeSize];
            for (int i = 0; i < largeSize; i++) {
                msg[i] = (byte) (i & 0xFF);
            }
            assertTrue(q.add(msg));
            byte[] result = q.get();
            assertNotNull(result);
            assertArrayEquals(msg, result);
        } finally {
            q.cleanup();
        }
    }

    // -----------------------------------------------------------------------
    //  Edge cases
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Queue with capacity 1 functions correctly")
    void singleSlotQueue() throws IOException {
        String path = "/tmp/test-mmf-single-slot";
        CircularMMFQueue q = new CircularMMFQueue(MSG_SIZE, 1, path);
        try {
            q.reset();

            assertTrue(q.add(createMessage(1)));
            assertTrue(q.isFull());
            assertFalse(q.add(createMessage(2)));

            byte[] r = q.get();
            assertNotNull(r);
            assertArrayEquals(createMessage(1), r);
            assertTrue(q.isEmpty());

            // Refill the single slot
            assertTrue(q.add(createMessage(3)));
            assertTrue(q.isFull());
        } finally {
            q.cleanup();
        }
    }

    @Test
    @DisplayName("Consume all then add more – queue remains operational after draining")
    void consumeAllThenAddMore() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.get();
        }
        assertTrue(queue.isEmpty());

        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            assertTrue(queue.add(createMessage(100 + i)));
        }
        assertTrue(queue.isFull());
        assertEquals(QUEUE_CAPACITY, queue.getQueueSize());
    }

    @Test
    @DisplayName("get() after queue has been drained returns null (not stale data)")
    void getAfterDrainReturnsNull() {
        queue.add(createMessage(1));
        queue.get();
        assertNull(queue.get(), "second get should return null, not stale data");
    }

    @Test
    @DisplayName("getWithAck/ack across wrap boundary preserves data integrity")
    void getWithAckAcrossWrap() {
        // First: fill and drain via ack protocol
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            byte[] msg = queue.getWithAck();
            assertNotNull(msg);
            queue.ack();
        }

        // Second: indices have wrapped – verify ack protocol still works
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(50 + i));
        }
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            byte[] msg = queue.getWithAck();
            assertNotNull(msg, "wrapped getWithAck should succeed for msg " + i);
            assertEquals((byte) (50 + i), msg[0]);
            queue.ack();
        }
        assertTrue(queue.isEmpty());
    }

    @Test
    @DisplayName("All-zero message round-trips correctly")
    void allZeroMessage() {
        byte[] zeros = new byte[MSG_SIZE]; // all zeros
        queue.add(zeros);
        byte[] result = queue.get();
        assertNotNull(result);
        assertArrayEquals(zeros, result);
    }

    @Test
    @DisplayName("All-0xFF message round-trips correctly")
    void allOnesMessage() {
        byte[] ones = new byte[MSG_SIZE];
        Arrays.fill(ones, (byte) 0xFF);
        queue.add(ones);
        byte[] result = queue.get();
        assertNotNull(result);
        assertArrayEquals(ones, result);
    }
}

