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

public class CircularMMFQueueVariableSize implements CircularMMFQueue {

    private static final String NAME = "OFF_HEAP_VARIABLE_QUEUE";

    private static final int WRITER_CONTEXT_BYTES = 64;
    private static final int READER_CONTEXT_OFFSET = 64;
    private static final int READER_CONTEXT_BYTES = 64;
    private static final int RING_OFFSET = READER_CONTEXT_OFFSET + READER_CONTEXT_BYTES;
    private static final int WRITE_POSITION_IN_CONTEXT = 0;
    private static final int CAPACITY_IN_CONTEXT = 8;
    private static final int MAX_MESSAGE_SIZE_IN_CONTEXT = 16;
    private static final int READ_POSITION_IN_CONTEXT = 0;

    private static final int RECORD_HEADER_BYTES = 16;
    private static final int LENGTH_IN_HEADER = 0;
    private static final int SEQUENCE_IN_HEADER = 8;
    private static final int PADDING_TO_END_OF_RING = -1;
    private static final int EMPTY = 0;
    private static final long NOTHING_TO_ACK = -1;

    private static final VarHandle LONG_IN_BUFFER = MethodHandles.byteBufferViewVarHandle(long[].class, LITTLE_ENDIAN);

    private static final Logger LOG = LoggerFactory.getLogger(CircularMMFQueueVariableSize.class);

    private final int maxMessageSize;
    private final int capacity;
    private final int ringMask;
    private final RandomAccessFile queue;
    private final FileChannel queueChannel;
    private final MappedByteBuffer writerContext;
    private final MappedByteBuffer readerContext;
    private final ByteBuffer writerRing;
    private final ByteBuffer readerRing;
    private final String queuePath;

    private long writePosition;
    private long nextSequenceToWrite;
    private long lastSeenReadPosition;

    private long readPosition;
    private long lastSeenWritePosition;
    private long positionAfterUnackedMessage = NOTHING_TO_ACK;
    private int nextMessageOffset;
    private int nextMessageLength;
    private long positionAfterNextMessage;

    public CircularMMFQueueVariableSize(int maxMessageSize, int capacityBytes, String path) throws IOException {

        requireValidSizes(maxMessageSize, capacityBytes);
        this.maxMessageSize = maxMessageSize;
        this.capacity = capacityBytes;
        this.ringMask = capacityBytes - 1;

        createDirectory(path);
        queuePath = path + "/" + NAME + ".txt";
        queue = new RandomAccessFile(queuePath, "rw");
        queueChannel = queue.getChannel();

        writerContext = queueChannel.map(READ_WRITE, 0, WRITER_CONTEXT_BYTES);
        writerContext.order(LITTLE_ENDIAN);
        readerContext = queueChannel.map(READ_WRITE, READER_CONTEXT_OFFSET, READER_CONTEXT_BYTES);
        MappedByteBuffer ring = queueChannel.map(READ_WRITE, RING_OFFSET, capacityBytes);
        writerRing = ring.duplicate().order(LITTLE_ENDIAN);
        readerRing = ring.duplicate().order(LITTLE_ENDIAN);

        rememberSizesOnFirstOpenElseRequireSameSizes();
        resumeFromPublishedPositions();

        LOG.info("Variable size queue of {} bytes, max message {}, written {}, read {}", capacity, maxMessageSize, messagesWritten(), messagesRead());
    }

    @Override
    public boolean add(byte[] msg) {
        return add(msg, 0, msg.length);
    }

    @Override
        public boolean add(byte[] msg, int offset, int length) {

        requireValidMessageLength(length);
        int recordSize = recordSizeFor(length);
        int padding = paddingBeforeRecordAt(writePosition, recordSize);
        if (!readerHasFreedUpTo(writePosition + padding + recordSize + RECORD_HEADER_BYTES)) return false;

        int recordOffset = padding > 0 ? padRestOfRingAndStartOver() : offsetInRing(writePosition);
        leaveEmptyHeaderForNextRecord(offsetInRing(recordOffset + recordSize));
        writeRecord(recordOffset, msg, offset, length);
        publishWritePosition(writePosition + padding + recordSize);
        return true;
    }

    @Override
    public byte[] get() {

        byte[] msg = getWithAck();
        if (msg != null) {
            ack();
        }
        return msg;
    }

    @Override
    public byte[] getWithAck() {

        if (!findNextUnreadMessage()) return null;

        byte[] msg = copyNextMessage();
        positionAfterUnackedMessage = positionAfterNextMessage;
        return msg;
    }

    @Override
    public boolean readWithAck(MessageHandler handler) {

        if (!findNextUnreadMessage()) return false;

        positionAfterUnackedMessage = positionAfterNextMessage;
        handler.onMessage(readerRing, nextMessageOffset + RECORD_HEADER_BYTES, nextMessageLength);
        return true;
    }

    @Override
    public void ack() {
        if (isAwaitingAck()) {
            publishReadPosition(positionAfterUnackedMessage);
            positionAfterUnackedMessage = NOTHING_TO_ACK;
        }
    }

    @Override
    public boolean isFull() {
        long position = publishedWritePosition();
        int largestRecord = recordSizeFor(maxMessageSize);
        long endOfLargestRecord = position + paddingBeforeRecordAt(position, largestRecord) + largestRecord + RECORD_HEADER_BYTES;
        return endOfLargestRecord - publishedReadPosition() > capacity;
    }

    @Override
    public boolean isEmpty() {
        return readPosition >= publishedWritePosition();
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
        return sequenceAt(publishedWritePosition());
    }

    @Override
    public long messagesRead() {
        return sequenceAt(publishedReadPosition());
    }

    @Override
    public void reset() {
        writePosition = 0;
        nextSequenceToWrite = 0;
        lastSeenReadPosition = 0;
        readPosition = 0;
        lastSeenWritePosition = 0;
        positionAfterUnackedMessage = NOTHING_TO_ACK;
        writerRing.putLong(SEQUENCE_IN_HEADER, 0);
        writerRing.putInt(LENGTH_IN_HEADER, EMPTY);
        LONG_IN_BUFFER.setRelease(readerContext, READ_POSITION_IN_CONTEXT, 0L);
        LONG_IN_BUFFER.setRelease(writerContext, WRITE_POSITION_IN_CONTEXT, 0L);
    }

    @Override
    public void cleanup() {
        try {
            closeQueue();
            delete(Path.of(queuePath));
        } catch (IOException e) {
            LOG.error("Failed to delete underlying files", e);
        }
    }

    @Override
    public void closeQueue() {
        try {
            queueChannel.close();
            queue.close();
        } catch (IOException e) {
            LOG.error("Failed to close underlying file", e);
        }
    }

    @Override
    public void close() {
        closeQueue();
    }

    private int padRestOfRingAndStartOver() {
        writerRing.putInt(offsetInRing(writePosition) + LENGTH_IN_HEADER, PADDING_TO_END_OF_RING);
        return 0;
    }

    private void leaveEmptyHeaderForNextRecord(int nextRecordOffset) {
        writerRing.putLong(nextRecordOffset + SEQUENCE_IN_HEADER, nextSequenceToWrite + 1);
        writerRing.putInt(nextRecordOffset + LENGTH_IN_HEADER, EMPTY);
    }

    private void writeRecord(int recordOffset, byte[] msg, int offset, int length) {
        writerRing.position(recordOffset + RECORD_HEADER_BYTES);
        writerRing.put(msg, offset, length);
        writerRing.putLong(recordOffset + SEQUENCE_IN_HEADER, nextSequenceToWrite);
        writerRing.putInt(recordOffset + LENGTH_IN_HEADER, length);
    }

    private void publishWritePosition(long position) {
        writePosition = position;
        nextSequenceToWrite++;
        LONG_IN_BUFFER.setRelease(writerContext, WRITE_POSITION_IN_CONTEXT, position);
    }

    private boolean readerHasFreedUpTo(long position) {
        if (position - lastSeenReadPosition <= capacity) return true;
        lastSeenReadPosition = publishedReadPosition();
        return position - lastSeenReadPosition <= capacity;
    }

    private boolean findNextUnreadMessage() {

        if (isAwaitingAck() || !writerIsAheadOfReader()) return false;

        long recordPosition = skipPaddingAt(readPosition);
        int recordOffset = offsetInRing(recordPosition);
        int length = lengthAt(recordOffset);
        if (length <= EMPTY || length > maxMessageSize) {
            throw new IllegalStateException("Corrupt record at byte " + recordPosition + " of " + queuePath + ", length " + length);
        }
        nextMessageOffset = recordOffset;
        nextMessageLength = length;
        positionAfterNextMessage = recordPosition + recordSizeFor(length);
        return true;
    }

    private boolean writerIsAheadOfReader() {
        if (readPosition < lastSeenWritePosition) return true;
        lastSeenWritePosition = publishedWritePosition();
        return readPosition < lastSeenWritePosition;
    }

    private long skipPaddingAt(long position) {
        int offset = offsetInRing(position);
        return lengthAt(offset) == PADDING_TO_END_OF_RING ? position + capacity - offset : position;
    }

    private byte[] copyNextMessage() {
        byte[] msg = new byte[nextMessageLength];
        readerRing.position(nextMessageOffset + RECORD_HEADER_BYTES);
        readerRing.get(msg);
        return msg;
    }

    private boolean isAwaitingAck() {
        return positionAfterUnackedMessage != NOTHING_TO_ACK;
    }

    private void publishReadPosition(long position) {
        readPosition = position;
        LONG_IN_BUFFER.setRelease(readerContext, READ_POSITION_IN_CONTEXT, position);
    }

    private int paddingBeforeRecordAt(long position, int recordSize) {
        int offset = offsetInRing(position);
        boolean recordWouldCrossEndOfRing = offset + recordSize > capacity;
        return recordWouldCrossEndOfRing ? capacity - offset : 0;
    }

    private int lengthAt(int recordOffset) {
        return readerRing.getInt(recordOffset + LENGTH_IN_HEADER);
    }

    private long sequenceAt(long position) {
        return readerRing.getLong(offsetInRing(position) + SEQUENCE_IN_HEADER);
    }

    private int offsetInRing(long position) {
        return (int) position & ringMask;
    }

    private long publishedWritePosition() {
        return (long) LONG_IN_BUFFER.getAcquire(writerContext, WRITE_POSITION_IN_CONTEXT);
    }

    private long publishedReadPosition() {
        return (long) LONG_IN_BUFFER.getAcquire(readerContext, READ_POSITION_IN_CONTEXT);
    }

    private void resumeFromPublishedPositions() {
        writePosition = publishedWritePosition();
        nextSequenceToWrite = sequenceAt(writePosition);
        readPosition = publishedReadPosition();
        lastSeenReadPosition = readPosition;
        lastSeenWritePosition = writePosition;
        if (readPosition > writePosition) {
            throw new IllegalStateException("Invalid queue state, reader at " + readPosition + " is ahead of writer at " + writePosition);
        }
    }

    private void rememberSizesOnFirstOpenElseRequireSameSizes() {
        long storedCapacity = writerContext.getLong(CAPACITY_IN_CONTEXT);
        long storedMaxMessageSize = writerContext.getLong(MAX_MESSAGE_SIZE_IN_CONTEXT);
        boolean firstOpen = storedCapacity == 0;
        if (firstOpen) {
            writerContext.putLong(MAX_MESSAGE_SIZE_IN_CONTEXT, maxMessageSize);
            writerContext.putLong(CAPACITY_IN_CONTEXT, capacity);
            return;
        }
        if (storedCapacity != capacity || storedMaxMessageSize != maxMessageSize) {
            closeQueue();
            throw new IllegalStateException(queuePath + " was created with capacity " + storedCapacity + " and max message "
                    + storedMaxMessageSize + ", opened with " + capacity + " and " + maxMessageSize);
        }
    }

    private void requireValidMessageLength(int length) {
        if (length <= 0 || length > maxMessageSize) {
            throw new IllegalArgumentException("Expected 1 to " + maxMessageSize + " bytes, got " + length);
        }
    }

    private static void requireValidSizes(int maxMessageSize, int capacityBytes) {
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException("maxMessageSize must be positive, got " + maxMessageSize);
        }
        if (capacityBytes <= 0 || Integer.bitCount(capacityBytes) != 1) {
            throw new IllegalArgumentException("capacityBytes must be a positive power of two, got " + capacityBytes);
        }
        boolean ringHoldsTwoLargestRecords = maxMessageSize <= (capacityBytes >> 1) - 2 * RECORD_HEADER_BYTES;
        if (!ringHoldsTwoLargestRecords) {
            throw new IllegalArgumentException("capacityBytes " + capacityBytes + " must hold two records of " + maxMessageSize + " bytes");
        }
    }

    private static void createDirectory(String path) throws IOException {
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException(String.format("Failed to create directory structure %s", path));
        }
    }

    private static int recordSizeFor(int messageLength) {
        return (RECORD_HEADER_BYTES + messageLength + RECORD_HEADER_BYTES - 1) & -RECORD_HEADER_BYTES;
    }
}
