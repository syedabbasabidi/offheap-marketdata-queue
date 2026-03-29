package com.abidi.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;


public class CircularMMFQueueTest {

    private static final int MSG_SIZE = 64;
    private static final int QUEUE_CAPACITY = 8;
    private static final String QUEUE_PATH = "/tmp/test-mmf-queue";

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

    // -----------------------------------------------------------------------
    //  Constructor validation
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Constructor rejects zero msgSize")
    void constructorRejectsZeroMsgSize() {
        assertThrows(IllegalArgumentException.class,
                () -> new CircularMMFQueue(0, QUEUE_CAPACITY, "/tmp/test-mmf-invalid"));
    }

    @Test
    @DisplayName("Constructor rejects negative msgSize")
    void constructorRejectsNegativeMsgSize() {
        assertThrows(IllegalArgumentException.class,
                () -> new CircularMMFQueue(-1, QUEUE_CAPACITY, "/tmp/test-mmf-invalid"));
    }

    @Test
    @DisplayName("Constructor rejects zero queueCapacity")
    void constructorRejectsZeroCapacity() {
        assertThrows(IllegalArgumentException.class,
                () -> new CircularMMFQueue(MSG_SIZE, 0, "/tmp/test-mmf-invalid"));
    }

    @Test
    @DisplayName("Constructor rejects negative queueCapacity")
    void constructorRejectsNegativeCapacity() {
        assertThrows(IllegalArgumentException.class,
                () -> new CircularMMFQueue(MSG_SIZE, -5, "/tmp/test-mmf-invalid"));
    }

    // -----------------------------------------------------------------------
    //  add() message length validation
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("add() rejects message shorter than configured msgSize")
    void addRejectsShorterMessage() {
        byte[] tooShort = new byte[MSG_SIZE - 1];
        assertThrows(IllegalArgumentException.class, () -> queue.add(tooShort));
        assertEquals(0, queue.messagesWritten(), "writer index must not advance on rejected add");
    }

    @Test
    @DisplayName("add() rejects message longer than configured msgSize")
    void addRejectsLongerMessage() {
        byte[] tooLong = new byte[MSG_SIZE + 1];
        assertThrows(IllegalArgumentException.class, () -> queue.add(tooLong));
        assertEquals(0, queue.messagesWritten(), "writer index must not advance on rejected add");
    }

    @Test
    @DisplayName("add() rejects empty (zero-length) message when msgSize is 64")
    void addRejectsEmptyMessage() {
        byte[] empty = new byte[0];
        assertThrows(IllegalArgumentException.class, () -> queue.add(empty));
    }

    // -----------------------------------------------------------------------
    //  Orphaned ack scenario (getWithAck then get without acking)
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("get() after un-acked getWithAck re-reads the same message and advances reader")
    void getAfterUnAckedGetWithAckRereadsSameMessage() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));

        byte[] ackResult = Arrays.copyOf(queue.getWithAck(), MSG_SIZE);
        assertArrayEquals(createMessage(1), ackResult);
        assertEquals(0, queue.messagesRead(), "getWithAck should not advance reader");

        // Now call get() without acking — it reads the same message because readIndex hasn't moved
        byte[] getResult = queue.get();
        assertNotNull(getResult);
        assertArrayEquals(createMessage(1), getResult);
        assertEquals(1, queue.messagesRead(), "get() advances reader past the un-acked message");
    }

    @Test
    @DisplayName("Orphaned indexToAck blocks future getWithAck calls")
    void orphanedAckBlocksFutureGetWithAck() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));

        queue.getWithAck(); // sets indexToAck, doesn't advance reader
        queue.get();        // advances reader past message 1, orphans indexToAck

        // Now getWithAck should return null because indexToAck is still set (orphaned)
        assertNull(queue.getWithAck(),
                "getWithAck must be blocked while an orphaned indexToAck exists");
    }

    // -----------------------------------------------------------------------
    //  Double ack safety
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Double ack() after getWithAck is safe — only first ack advances reader")
    void doubleAckIsSafe() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));

        queue.getWithAck();
        queue.ack(); // first ack — advances reader
        assertEquals(1, queue.messagesRead());

        queue.ack(); // second ack — should be a no-op
        assertEquals(1, queue.messagesRead(), "second ack must not advance reader again");
    }

    // -----------------------------------------------------------------------
    //  Single-slot queue with getWithAck/ack
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Capacity-1 queue works with getWithAck/ack protocol")
    void singleSlotWithAckProtocol() throws IOException {
        String path = "/tmp/test-mmf-single-ack";
        CircularMMFQueue q = new CircularMMFQueue(MSG_SIZE, 1, path);
        try {
            q.reset();

            assertTrue(q.add(createMessage(10)));
            assertTrue(q.isFull());

            byte[] msg = q.getWithAck();
            assertNotNull(msg);
            assertArrayEquals(createMessage(10), msg);
            assertTrue(q.isFull(), "slot not freed until ack");

            q.ack();
            assertTrue(q.isEmpty());
            assertFalse(q.isFull());

            // Refill after ack
            assertTrue(q.add(createMessage(20)));
            assertTrue(q.isFull());
        } finally {
            q.cleanup();
        }
    }

    // -----------------------------------------------------------------------
    //  Persistence: indexToAck is in-memory only
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Pending ack state is lost across close/reopen — getWithAck works after reopen")
    void pendingAckLostOnReopen() throws IOException {
        queue.add(createMessage(1));
        queue.add(createMessage(2));
        queue.getWithAck(); // pending ack, readIndex not advanced

        queue.closeQueue();
        queue = new CircularMMFQueue(MSG_SIZE, QUEUE_CAPACITY, QUEUE_PATH);

        // After reopen, indexToAck is reset to -1 (fresh instance)
        // Reader index is still 0 (was never advanced), writer at 2
        assertEquals(0, queue.messagesRead());
        assertEquals(2, queue.messagesWritten());

        // getWithAck should work — no stale pending ack
        byte[] msg = queue.getWithAck();
        assertNotNull(msg, "getWithAck should work after reopen clears in-memory ack state");
        assertArrayEquals(createMessage(1), msg);
        queue.ack();
        assertEquals(1, queue.messagesRead());
    }

    // -----------------------------------------------------------------------
    //  Persistence after wrap-around
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Persistence after wrap-around — indices and data survive close/reopen")
    void persistenceAfterWrap() throws IOException {
        // Fill and drain once (indices wrap)
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.get();
        }

        // Add some more messages (post-wrap)
        queue.add(createMessage(80));
        queue.add(createMessage(81));

        queue.closeQueue();
        queue = new CircularMMFQueue(MSG_SIZE, QUEUE_CAPACITY, QUEUE_PATH);

        assertEquals(QUEUE_CAPACITY, queue.messagesRead());
        assertEquals(QUEUE_CAPACITY + 2, queue.messagesWritten());
        assertEquals(2, queue.getQueueSize());

        byte[] r1 = Arrays.copyOf(queue.get(), MSG_SIZE);
        byte[] r2 = queue.get();
        assertArrayEquals(createMessage(80), r1);
        assertArrayEquals(createMessage(81), r2);
        assertTrue(queue.isEmpty());
    }

    // -----------------------------------------------------------------------
    //  Stress: many wrap-around cycles
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Stress: 100 fill/drain cycles with data integrity checks")
    void manyWrapCycles() {
        for (int cycle = 0; cycle < 100; cycle++) {
            for (int i = 0; i < QUEUE_CAPACITY; i++) {
                assertTrue(queue.add(createMessage((cycle + i) & 0x7F)),
                        "cycle " + cycle + " add " + i);
            }
            assertTrue(queue.isFull());

            for (int i = 0; i < QUEUE_CAPACITY; i++) {
                byte[] r = queue.get();
                assertNotNull(r, "cycle " + cycle + " get " + i);
                assertEquals((byte) ((cycle + i) & 0x7F), r[0],
                        "data mismatch at cycle " + cycle + " msg " + i);
            }
            assertTrue(queue.isEmpty());
        }

        // Counters should reflect total throughput
        assertEquals(100L * QUEUE_CAPACITY, queue.messagesWritten());
        assertEquals(100L * QUEUE_CAPACITY, queue.messagesRead());
        assertEquals(0, queue.getQueueSize());
    }

    // -----------------------------------------------------------------------
    //  Partial fill/drain cycles
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Partial consume then refill — queue tracks size correctly")
    void partialConsumeThenRefill() {
        // Add 6 messages
        for (int i = 0; i < 6; i++) {
            queue.add(createMessage(i));
        }
        assertEquals(6, queue.getQueueSize());

        // Consume 3
        for (int i = 0; i < 3; i++) {
            byte[] r = queue.get();
            assertNotNull(r);
            assertEquals((byte) i, r[0]);
        }
        assertEquals(3, queue.getQueueSize());

        // Refill to capacity: 3 existing + 5 new = 8 = QUEUE_CAPACITY
        for (int i = 0; i < 5; i++) {
            assertTrue(queue.add(createMessage(100 + i)));
        }
        assertTrue(queue.isFull());
        assertEquals(QUEUE_CAPACITY, queue.getQueueSize());

        // Can't add more
        assertFalse(queue.add(createMessage(99)));

        // Drain and verify FIFO order: messages 3,4,5,100,101,102,103,104
        for (int i = 3; i < 6; i++) {
            byte[] r = queue.get();
            assertNotNull(r);
            assertEquals((byte) i, r[0]);
        }
        for (int i = 0; i < 5; i++) {
            byte[] r = queue.get();
            assertNotNull(r);
            assertEquals((byte) (100 + i), r[0]);
        }
        assertTrue(queue.isEmpty());
    }

    // -----------------------------------------------------------------------
    //  getQueueSize with getWithAck (un-acked message still counted)
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("getQueueSize includes un-acked message (reader not advanced)")
    void queueSizeIncludesUnAckedMessage() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));
        assertEquals(2, queue.getQueueSize());

        queue.getWithAck();
        assertEquals(2, queue.getQueueSize(), "un-acked message still counts toward queue size");

        queue.ack();
        assertEquals(1, queue.getQueueSize(), "queue size decreases after ack");
    }

    // -----------------------------------------------------------------------
    //  isFull with getWithAck (un-acked message holds the slot)
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("isFull still true when pending ack — reader hasn't advanced to free slot")
    void isFullWithPendingAck() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        assertTrue(queue.isFull());

        queue.getWithAck(); // read but don't ack — slot not freed
        assertTrue(queue.isFull(), "queue still full because reader index hasn't advanced");

        queue.ack(); // now free the slot
        assertFalse(queue.isFull());
        assertTrue(queue.add(createMessage(99)), "can add after ack frees a slot");
    }

    // -----------------------------------------------------------------------
    //  Multiple resets
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Multiple consecutive resets leave queue in a clean state")
    void multipleResets() {
        queue.add(createMessage(1));
        queue.reset();
        queue.reset();
        queue.reset();

        assertTrue(queue.isEmpty());
        assertEquals(0, queue.messagesWritten());
        assertEquals(0, queue.messagesRead());

        // Queue still usable
        assertTrue(queue.add(createMessage(42)));
        byte[] result = queue.get();
        assertNotNull(result);
        assertArrayEquals(createMessage(42), result);
    }

    @Test
    @DisplayName("Reset mid-fill then refill to full capacity works")
    void resetMidFillThenRefill() {
        queue.add(createMessage(1));
        queue.add(createMessage(2));
        queue.add(createMessage(3));
        queue.reset();

        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            assertTrue(queue.add(createMessage(50 + i)));
        }
        assertTrue(queue.isFull());
        assertEquals(QUEUE_CAPACITY, queue.getQueueSize());

        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            byte[] r = queue.get();
            assertNotNull(r);
            assertEquals((byte) (50 + i), r[0]);
        }
        assertTrue(queue.isEmpty());
    }

    // -----------------------------------------------------------------------
    //  AutoCloseable / try-with-resources
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Queue implements AutoCloseable and can be used in try-with-resources")
    void autoCloseable() throws IOException {
        String path = "/tmp/test-mmf-autocloseable";
        try (CircularMMFQueue q = new CircularMMFQueue(MSG_SIZE, QUEUE_CAPACITY, path)) {
            q.reset();
            q.add(createMessage(1));
            byte[] result = q.get();
            assertNotNull(result);
            assertArrayEquals(createMessage(1), result);
        }
        // After close, reopening should still find persisted data structure
        // (the files still exist until cleanup is called)
    }

    // -----------------------------------------------------------------------
    //  Large monotonic counters
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Monotonic counters grow correctly over many one-at-a-time add/get cycles")
    void largeMonotonicCounters() {
        int totalMessages = 200;
        for (int i = 0; i < totalMessages; i++) {
            assertTrue(queue.add(createMessage(i & 0x7F)));
            assertNotNull(queue.get());
        }
        assertEquals(totalMessages, queue.messagesWritten());
        assertEquals(totalMessages, queue.messagesRead());
        assertEquals(0, queue.getQueueSize());
        assertTrue(queue.isEmpty());
    }

    // -----------------------------------------------------------------------
    //  getQueueSize never negative
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("getQueueSize is never negative through varied operations")
    void queueSizeNeverNegative() {
        assertTrue(queue.getQueueSize() >= 0);

        queue.add(createMessage(1));
        assertTrue(queue.getQueueSize() >= 0);

        queue.get();
        assertTrue(queue.getQueueSize() >= 0);

        // Extra get on empty — returns null, size should stay at 0
        queue.get();
        assertTrue(queue.getQueueSize() >= 0);
        assertEquals(0, queue.getQueueSize());
    }

    // -----------------------------------------------------------------------
    //  Boundary: fill exactly to capacity, drain exactly, repeat
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Alternating single add/get maintains correct state")
    void alternatingSingleAddGet() {
        for (int i = 0; i < 50; i++) {
            assertTrue(queue.add(createMessage(i & 0x7F)));
            assertEquals(1, queue.getQueueSize());
            assertFalse(queue.isEmpty());

            byte[] r = queue.get();
            assertNotNull(r);
            assertEquals((byte) (i & 0x7F), r[0]);
            assertEquals(0, queue.getQueueSize());
            assertTrue(queue.isEmpty());
        }
        assertEquals(50, queue.messagesWritten());
        assertEquals(50, queue.messagesRead());
    }

    // -----------------------------------------------------------------------
    //  getWithAck full cycle across wrap with data integrity
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("getWithAck/ack one-at-a-time for many messages across wraps")
    void getWithAckManyMessagesAcrossWraps() {
        int totalMessages = QUEUE_CAPACITY * 5;
        for (int i = 0; i < totalMessages; i++) {
            assertTrue(queue.add(createMessage(i & 0x7F)));
            byte[] msg = queue.getWithAck();
            assertNotNull(msg, "getWithAck failed at message " + i);
            assertEquals((byte) (i & 0x7F), msg[0]);
            queue.ack();
        }
        assertEquals(totalMessages, queue.messagesWritten());
        assertEquals(totalMessages, queue.messagesRead());
        assertTrue(queue.isEmpty());
    }

    // -----------------------------------------------------------------------
    //  Two independent queues at different paths
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("Two queues at different paths operate independently")
    void twoIndependentQueues() throws IOException {
        String path2 = "/tmp/test-mmf-queue-independent";
        CircularMMFQueue q2 = new CircularMMFQueue(MSG_SIZE, QUEUE_CAPACITY, path2);
        try {
            q2.reset();

            queue.add(createMessage(1));
            q2.add(createMessage(99));

            assertEquals(1, queue.getQueueSize());
            assertEquals(1, q2.getQueueSize());

            byte[] r1 = queue.get();
            byte[] r2 = q2.get();

            assertArrayEquals(createMessage(1), r1);
            assertArrayEquals(createMessage(99), r2);
            assertTrue(queue.isEmpty());
            assertTrue(q2.isEmpty());
        } finally {
            q2.cleanup();
        }
    }

    // -----------------------------------------------------------------------
    //  Ack on full queue frees exactly one slot
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("getWithAck + ack on full queue frees exactly one slot for a new add")
    void ackOnFullQueueFreesOneSlot() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            queue.add(createMessage(i));
        }
        assertTrue(queue.isFull());

        byte[] msg = queue.getWithAck();
        assertNotNull(msg);
        assertEquals((byte) 0, msg[0]);

        // Still full — ack hasn't happened
        assertTrue(queue.isFull());
        assertFalse(queue.add(createMessage(50)));

        queue.ack();
        assertFalse(queue.isFull());
        assertTrue(queue.add(createMessage(50)));

        // Verify data integrity for remaining messages
        for (int i = 1; i < QUEUE_CAPACITY; i++) {
            byte[] r = queue.get();
            assertNotNull(r);
            assertEquals((byte) i, r[0], "message " + i);
        }
        // Last message is the newly added one
        byte[] last = queue.get();
        assertNotNull(last);
        assertArrayEquals(createMessage(50), last);
        assertTrue(queue.isEmpty());
    }
}

