package com.abidi.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.Files.delete;

/**
 * Off heap circular queue of variable length messages, each stored next to its own length.
 * <p>
 * Data ring of {@code capacityBytes} (power of two), every record 16 byte aligned:
 * <pre>
 * [int length][int unused][long sequence][payload ... padded to 16]
 * </pre>
 * <ul>
 *     <li>A record never wraps: if it doesn't fit before the end of the ring, a padding header (length -1) fills the
 *     rest and the record starts at offset 0.</li>
 *     <li>The writer always leaves an empty header (length 0) holding the next sequence where the next record will go,
 *     so the message count at any byte position is read from the header there. Writer and reader each publish a single
 *     long (their byte position), which keeps both crash consistent without a separate counter.</li>
 *     <li>Producer publishes its position with release after writing the record, consumer reads it with acquire, and
 *     vice versa for the consumer position, as in {@link CircularMMFQueueFixedSize}.</li>
 * </ul>
 * Capacity must hold at least two records of {@code maxMessageSize}, so a record plus wrap padding always fits an empty
 * queue. Both sides must open the queue with the same capacity and max message size; this is checked on open.
 */
public class CircularMMFQueueVariableSize implements CircularMMFQueue {

    public static final int HEADER = 16;
    public static final int WRITER_CONTEXT_BYTES = 64;

    private static final String NAME = "OFF_HEAP_VARIABLE_QUEUE";
    private static final int LENGTH_OFFSET = 0;
    private static final int SEQUENCE_OFFSET = 8;
    private static final int PADDING = -1;
    private static final int STORED_CAPACITY = 8;
    private static final int STORED_MAX_MESSAGE_SIZE = 16;

    static final VarHandle VH_LONG = MethodHandles.byteBufferViewVarHandle(long[].class, LITTLE_ENDIAN);

    private static final Logger LOG = LoggerFactory.getLogger(CircularMMFQueueVariableSize.class);

    private final int maxMessageSize;
    private final int capacity;
    private final int mask;
    private final RandomAccessFile queue;
    private final RandomAccessFile queueReaderContext;
    private final FileChannel queueChannel;
    private final FileChannel queueReaderContextChannel;
    private final MappedByteBuffer writerContextBuffer;
    private final MappedByteBuffer readerContextBuffer;
    private final ByteBuffer writeView;
    private final ByteBuffer readView;
    private final String queuePath;
    private final String queueReaderContextPath;

    private long writePos;
    private long writeSequence;
    private long cachedReadPos;

    private long readPos;
    private long cachedWritePos;
    private long ackPos = -1;
    private int peekOffset;
    private int peekLength;
    private long peekNext;

    public CircularMMFQueueVariableSize(int maxMessageSize, int capacityBytes, String path) throws IOException {

        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException("maxMessageSize must be positive, got " + maxMessageSize);
        }
        if (capacityBytes <= 0 || Integer.bitCount(capacityBytes) != 1) {
            throw new IllegalArgumentException("capacityBytes must be a positive power of two, got " + capacityBytes);
        }
        if (maxMessageSize > (capacityBytes >> 1) - 2 * HEADER) {
            throw new IllegalArgumentException("capacityBytes " + capacityBytes + " must hold two records of " + maxMessageSize + " bytes");
        }
        this.maxMessageSize = maxMessageSize;
        this.capacity = capacityBytes;
        this.mask = capacityBytes - 1;

        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException(String.format("Failed to create directory structure %s", path));
        }

        queuePath = path + "/" + NAME + ".txt";
        queueReaderContextPath = path + "/" + NAME + "-reader-context.txt";
        queue = new RandomAccessFile(queuePath, "rw");
        queueReaderContext = new RandomAccessFile(queueReaderContextPath, "rw");
        queueChannel = queue.getChannel();
        queueReaderContextChannel = queueReaderContext.getChannel();

        writerContextBuffer = queueChannel.map(READ_WRITE, 0, WRITER_CONTEXT_BYTES);
        writerContextBuffer.order(LITTLE_ENDIAN);
        readerContextBuffer = queueReaderContextChannel.map(READ_WRITE, 0, Long.BYTES);
        MappedByteBuffer data = queueChannel.map(READ_WRITE, WRITER_CONTEXT_BYTES, capacityBytes);
        writeView = data.duplicate().order(LITTLE_ENDIAN);
        readView = data.duplicate().order(LITTLE_ENDIAN);

        try {
            checkLayout();
        } catch (RuntimeException e) {
            closeQueue();
            throw e;
        }

        writePos = writerPosition();
        writeSequence = sequenceAt(writePos);
        readPos = readerPosition();
        cachedReadPos = readPos;
        cachedWritePos = writePos;

        if (readPos > writePos) {
            throw new IllegalStateException("Invalid queue state, reader at " + readPos + " is ahead of writer at " + writePos);
        }

        LOG.info("Variable size queue of {} bytes, max message {}, written {}, read {}", capacity, maxMessageSize, messagesWritten(), messagesRead());
    }

    @Override
    public boolean add(byte[] msg) {
        return add(msg, 0, msg.length);
    }

    @Override
    public boolean add(byte[] msg, int offset, int length) {

        if (length <= 0 || length > maxMessageSize) {
            throw new IllegalArgumentException("Expected 1 to " + maxMessageSize + " bytes, got " + length);
        }
        int size = recordSize(length);
        int at = (int) writePos & mask;
        int padding = at + size > capacity ? capacity - at : 0;
        if (!hasRoom(writePos + padding + size + HEADER)) return false;

        if (padding > 0) {
            writeView.putInt(at + LENGTH_OFFSET, PADDING);
            at = 0;
        }
        int next = (at + size) & mask;
        writeView.putLong(next + SEQUENCE_OFFSET, writeSequence + 1);
        writeView.putInt(next + LENGTH_OFFSET, 0);

        writeView.position(at + HEADER);
        writeView.put(msg, offset, length);
        writeView.putLong(at + SEQUENCE_OFFSET, writeSequence);
        writeView.putInt(at + LENGTH_OFFSET, length);

        writePos += padding + size;
        writeSequence++;
        VH_LONG.setRelease(writerContextBuffer, 0, writePos);
        return true;
    }

    /**
     * Allocates an array of the message length, use {@link #readWithAck(MessageHandler)} to avoid it.
     */
    @Override
    public byte[] get() {

        byte[] msg = getWithAck();
        if (msg != null) {
            ack();
        }
        return msg;
    }

    /**
     * Allocates an array of the message length, use {@link #readWithAck(MessageHandler)} to avoid it.
     */
    @Override
    public byte[] getWithAck() {

        if (!peek()) return null;

        byte[] msg = new byte[peekLength];
        readView.position(peekOffset + HEADER);
        readView.get(msg);
        ackPos = peekNext;
        return msg;
    }

    @Override
    public boolean readWithAck(MessageHandler handler) {

        if (!peek()) return false;

        ackPos = peekNext;
        handler.onMessage(readView, peekOffset + HEADER, peekLength);
        return true;
    }

    @Override
    public void ack() {
        if (ackPos != -1) {
            readPos = ackPos;
            ackPos = -1;
            VH_LONG.setRelease(readerContextBuffer, 0, readPos);
        }
    }

    @Override
    public boolean isFull() {
        long position = writerPosition();
        int size = recordSize(maxMessageSize);
        int at = (int) position & mask;
        int padding = at + size > capacity ? capacity - at : 0;
        return position + padding + size + HEADER - readerPosition() > capacity;
    }

    @Override
    public boolean isEmpty() {
        return readPos >= writerPosition();
    }

    @Override
    public int maxMessageSize() {
        return maxMessageSize;
    }

    @Override
    public long getQueueSize() {
        return messagesWritten() - messagesRead();
    }

    @Override
    public long messagesWritten() {
        return sequenceAt(writerPosition());
    }

    @Override
    public long messagesRead() {
        return sequenceAt(readerPosition());
    }

    @Override
    public void reset() {
        writePos = 0;
        writeSequence = 0;
        cachedReadPos = 0;
        readPos = 0;
        cachedWritePos = 0;
        ackPos = -1;
        writeView.putLong(SEQUENCE_OFFSET, 0);
        writeView.putInt(LENGTH_OFFSET, 0);
        VH_LONG.setRelease(readerContextBuffer, 0, 0L);
        VH_LONG.setRelease(writerContextBuffer, 0, 0L);
    }

    @Override
    public void cleanup() {
        try {
            closeQueue();
            delete(Path.of(queuePath));
            delete(Path.of(queueReaderContextPath));
        } catch (IOException e) {
            LOG.error("Failed to delete underlying files", e);
        }
    }

    @Override
    public void closeQueue() {
        try {
            queueChannel.close();
            queueReaderContextChannel.close();
            queue.close();
            queueReaderContext.close();
        } catch (IOException e) {
            LOG.error("Failed to close underlying file", e);
        }
    }

    @Override
    public void close() {
        closeQueue();
    }

    /**
     * Finds the next record, skipping wrap padding; false if empty or the previous record awaits {@link #ack()}.
     */
    private boolean peek() {

        if (ackPos != -1) return false;
        if (readPos >= cachedWritePos) {
            cachedWritePos = writerPosition();
            if (readPos >= cachedWritePos) return false;
        }
        long start = readPos;
        int at = (int) start & mask;
        int length = readView.getInt(at + LENGTH_OFFSET);
        if (length == PADDING) {
            start += capacity - at;
            at = 0;
            length = readView.getInt(LENGTH_OFFSET);
        }
        if (length <= 0 || length > maxMessageSize) {
            throw new IllegalStateException("Corrupt record at byte " + start + " of " + queuePath + ", length " + length);
        }
        peekOffset = at;
        peekLength = length;
        peekNext = start + recordSize(length);
        return true;
    }

    private boolean hasRoom(long end) {
        if (end - cachedReadPos <= capacity) return true;
        cachedReadPos = readerPosition();
        return end - cachedReadPos <= capacity;
    }

    private long sequenceAt(long position) {
        return readView.getLong(((int) position & mask) + SEQUENCE_OFFSET);
    }

    private long writerPosition() {
        return (long) VH_LONG.getAcquire(writerContextBuffer, 0);
    }

    private long readerPosition() {
        return (long) VH_LONG.getAcquire(readerContextBuffer, 0);
    }

    private void checkLayout() {
        long storedCapacity = writerContextBuffer.getLong(STORED_CAPACITY);
        long storedMax = writerContextBuffer.getLong(STORED_MAX_MESSAGE_SIZE);
        if (storedCapacity == 0) {
            writerContextBuffer.putLong(STORED_MAX_MESSAGE_SIZE, maxMessageSize);
            writerContextBuffer.putLong(STORED_CAPACITY, capacity);
        } else if (storedCapacity != capacity || storedMax != maxMessageSize) {
            throw new IllegalStateException(queuePath + " was created with capacity " + storedCapacity + " and max message "
                    + storedMax + ", opened with " + capacity + " and " + maxMessageSize);
        }
    }

    private static int recordSize(int length) {
        return (HEADER + length + HEADER - 1) & -HEADER;
    }
}
