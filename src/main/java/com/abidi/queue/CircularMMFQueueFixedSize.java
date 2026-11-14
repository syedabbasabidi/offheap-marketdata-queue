package com.abidi.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.Files.delete;

/**
 * Off heap circular queue, relies on linux dirty cache page mechanism to persist MMF.
 * Queue tested for single digit micro-second at 99.9 percentile.
 * linux dirty page frequency tuned to 50:
 * <p>
 * sysctl -w vm.dirty_writeback_centisecs=50
 * <p>
 * Disable write caching
 * <p>
 * sudo hdparm -W0 /dev/sdX  # Disable write caching
 */

public class CircularMMFQueueFixedSize implements CircularMMFQueue {

    private static final String NAME = "OFF_HEAP_QUEUE";
    public static final int TOTAL_BYTES_REQUIRED_FOR_WRITER_CONTEXT_WITH_PADDING = 64;
    private static final int MSG_SIZE_IN_CONTEXT = 8;
    private static final int CAPACITY_IN_CONTEXT = 16;
    private static final int READER_CONTEXT_OFFSET = 64;
    private static final int READER_CONTEXT_BYTES = 64;
    private static final int RING_OFFSET = READER_CONTEXT_OFFSET + READER_CONTEXT_BYTES;

    private final int msgLength;
    private final long queueCapacity;
    private final RandomAccessFile queue;
    private final MappedByteBuffer writerContextBuffer;
    private final MappedByteBuffer[] queueBuffers;
    private final MappedByteBuffer readerContextBuffer;
    private final FileChannel queueChannel;
    private final int numberOfMessagesPerBuffer;
    private final int bufferIndexShift;

    static final VarHandle VH_LONG = MethodHandles.byteBufferViewVarHandle(long[].class, BIG_ENDIAN);

    private long writeIndex;
    private int writeSlot;
    private long lastSeenReaderIndex;

    private long readIndex;
    private int readSlot;
    private long lastSeenWriterIndex;

    private final byte[] lastDequedMsg;
    private final String queuePath;

    private static final Logger LOG = LoggerFactory.getLogger(CircularMMFQueueFixedSize.class);
    private int indexToAck = -1;

    public CircularMMFQueueFixedSize(int msgSize, int queueCapacity, String path) throws IOException {

        if (msgSize <= 0) {
            throw new IllegalArgumentException("msgSize must be positive, got " + msgSize);
        }
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be positive, got " + queueCapacity);
        }

        this.msgLength = msgSize;
        this.queueCapacity = queueCapacity;

        createDirectoryStructureIfDoesntExist(path);

        queuePath = path + "/" + NAME + ".txt";
        queue = new RandomAccessFile(queuePath, "rw");

        queueChannel = queue.getChannel();
        writerContextBuffer = queueChannel.map(READ_WRITE, 0, TOTAL_BYTES_REQUIRED_FOR_WRITER_CONTEXT_WITH_PADDING);
        readerContextBuffer = queueChannel.map(READ_WRITE, READER_CONTEXT_OFFSET, READER_CONTEXT_BYTES);
        rememberSizesOnFirstOpenElseRequireSameSizes();
        numberOfMessagesPerBuffer = Integer.highestOneBit(Integer.MAX_VALUE / msgSize);
        bufferIndexShift = Integer.numberOfTrailingZeros(numberOfMessagesPerBuffer);
        queueBuffers = initializedBuffers(msgSize, queueCapacity);
        writeIndex = currentWriterIndex();
        writeSlot = (int) (writeIndex % queueCapacity);
        lastSeenReaderIndex = currentReaderIndex();
        readIndex = currentReaderIndex();
        readSlot = (int) (readIndex % queueCapacity);
        lastSeenWriterIndex = writeIndex;

        lastDequedMsg = new byte[msgSize];

        if (isReaderIndexAheadOfWriters()) {
            throw new RuntimeException("Invalid queue state");
        }

        LOG.info("Queue is setup with size {}, reader is at {}, writer is at {}, queue-size {}", queueCapacity, readIndex, writeIndex, getQueueSize());
    }

    @Override
    public byte[] get() {

        if (!writerIsAheadOfReader()) return null;

        readNextMessage();
        updateReaderContext();
        return lastDequedMsg;
    }

    @Override
    public boolean add(byte[] msg) {

        if (msg.length != msgLength) {
            throw new IllegalArgumentException("Expected message of " + msgLength + " bytes, got " + msg.length);
        }
        return add(msg, 0, msg.length);
    }

    @Override
    public boolean add(byte[] msg, int offset, int length) {

        if (length <= 0 || length > msgLength) {
            throw new IllegalArgumentException("Expected 1 to " + msgLength + " bytes, got " + length);
        }
        if (!readerHasFreedASlot()) return false;

        MappedByteBuffer buffer = queueBuffers[getBufferIndexOf(writeSlot)];
        buffer.position(getIndexWithinBuffer(writeSlot));
        buffer.put(msg, offset, length);
        updateWriterContext();
        return true;
    }

    @Override
    public boolean readWithAck(MessageHandler handler) {

        if (isAMsgAwaitingAck()) return false;
        if (!writerIsAheadOfReader()) return false;

        indexToAck = readSlot;
        handler.onMessage(queueBuffers[getBufferIndexOf(readSlot)], getIndexWithinBuffer(readSlot), msgLength);
        return true;
    }


    @Override
    public byte[] getWithAck() {

        if (isAMsgAwaitingAck()) return null;
        if (!writerIsAheadOfReader()) return null;

        indexToAck = readSlot;
        readNextMessage();
        return lastDequedMsg;
    }


    private void readNextMessage() {
        MappedByteBuffer buffer = queueBuffers[getBufferIndexOf(readSlot)];
        buffer.position(getIndexWithinBuffer(readSlot));
        buffer.get(lastDequedMsg, 0, msgLength);
    }

    private boolean readerHasFreedASlot() {
        if (writeIndex - lastSeenReaderIndex < queueCapacity) return true;
        lastSeenReaderIndex = currentReaderIndex();
        return writeIndex - lastSeenReaderIndex < queueCapacity;
    }

    private boolean writerIsAheadOfReader() {
        if (readIndex < lastSeenWriterIndex) return true;
        lastSeenWriterIndex = currentWriterIndex();
        return readIndex < lastSeenWriterIndex;
    }

    @Override
    public boolean isFull() {
        return (writeIndex - currentReaderIndex()) >= queueCapacity;
    }

    @Override
    public boolean isEmpty() {
        return readIndex >= currentWriterIndex();
    }

    private boolean isReaderIndexAheadOfWriters() {
        if (currentReaderIndex() > currentWriterIndex()) {
            LOG.error("Queue is invalid state, reader seems to have read more messages than written");
            return true;
        }
        return false;
    }

    private boolean isAMsgAwaitingAck() {
        if (indexToAck != -1) {
            LOG.info("Index {} hasn't been acked yet", indexToAck);
            return true;
        }
        return false;
    }

    @Override
    public void ack() {
        if (indexToAck == readSlot) {
            updateReaderContext();
            indexToAck = -1;
        }
    }

    private int nextSlot(int slot) {
        int next = slot + 1;
        return next == queueCapacity ? 0 : next;
    }

    private int getBufferIndexOf(int slot) {
        return slot >>> bufferIndexShift;
    }

    private int getIndexWithinBuffer(int slot) {
        return (slot & (numberOfMessagesPerBuffer - 1)) * msgLength;
    }


    private void updateReaderContext() {
        VH_LONG.setRelease(readerContextBuffer, 0, readIndex + 1);
        readIndex++;
        readSlot = nextSlot(readSlot);
    }

    private long currentReaderIndex() {
        return (long) VH_LONG.getAcquire(readerContextBuffer, 0);
    }

    private void updateWriterContext() {
        VH_LONG.setRelease(writerContextBuffer, 0, writeIndex + 1);
        writeIndex++;
        writeSlot = nextSlot(writeSlot);
    }

    private long currentWriterIndex() {
        return (long) VH_LONG.getAcquire(writerContextBuffer, 0);
    }

    @Override
    public int maxMessageSize() {
        return msgLength;
    }

    @Override
    public long getQueueSize() {
        return currentWriterIndex() - currentReaderIndex();
    }

    @Override
    public long messagesWritten() {
        return currentWriterIndex();
    }

    @Override
    public long messagesRead() {
        return currentReaderIndex();
    }

    private void rememberSizesOnFirstOpenElseRequireSameSizes() throws IOException {
        long storedMsgSize = writerContextBuffer.getLong(MSG_SIZE_IN_CONTEXT);
        long storedCapacity = writerContextBuffer.getLong(CAPACITY_IN_CONTEXT);
        boolean firstOpen = storedMsgSize == 0;
        if (firstOpen) {
            writerContextBuffer.putLong(MSG_SIZE_IN_CONTEXT, msgLength);
            writerContextBuffer.putLong(CAPACITY_IN_CONTEXT, queueCapacity);
            return;
        }
        if (storedMsgSize != msgLength || storedCapacity != queueCapacity) {
            closeQueue();
            throw new IllegalStateException(queuePath + " was created with msgSize " + storedMsgSize
                    + " and capacity " + storedCapacity + ", opened with " + msgLength + " and " + queueCapacity);
        }
    }

    private MappedByteBuffer[] initializedBuffers(long msgSize, int queueCapacity) throws IOException {

        int numberOfBuffers = (queueCapacity + numberOfMessagesPerBuffer - 1) / numberOfMessagesPerBuffer;
        MappedByteBuffer[] queueBuffers = new MappedByteBuffer[numberOfBuffers];
        long bytesPerBuffer = msgSize * numberOfMessagesPerBuffer;

        for (int bufferIndex = 0; bufferIndex < numberOfBuffers; bufferIndex++) {
            int messagesInBuffer = Math.min(numberOfMessagesPerBuffer, queueCapacity - bufferIndex * numberOfMessagesPerBuffer);
            long bufferStartInFile = RING_OFFSET + bufferIndex * bytesPerBuffer;
            queueBuffers[bufferIndex] = queueChannel.map(READ_WRITE, bufferStartInFile, msgSize * messagesInBuffer);
        }
        return queueBuffers;
    }

    private static void createDirectoryStructureIfDoesntExist(String path) throws IOException {
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException(String.format("Failed to create directory structure %s", path));
        }
    }

    @Override
    public void reset() {
        this.readIndex = 0;
        this.readSlot = 0;
        this.lastSeenWriterIndex = 0;
        this.writeIndex = 0;
        this.writeSlot = 0;
        this.lastSeenReaderIndex = 0;
        this.indexToAck = -1;
        VH_LONG.setRelease(readerContextBuffer, 0, readIndex);
        VH_LONG.setRelease(writerContextBuffer, 0, writeIndex);
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
}