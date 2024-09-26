package com.abidi.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.Files.delete;
import static java.util.Arrays.stream;

/**
 * Off heap circular queue, relies on linux dirty cache page mechanism to persist MMF.
 * Queue tested for single digit micro-second at 99.9 percentile.
 * linux dirty page frequency tuned to 50:  sysctl -w vm.dirty_writeback_centisecs=50
 */

public class CircularMMFQueue {

    public static final int DEFAULT_SIZE = 100_000;
    private static final String NAME = "OFF_HEAP_QUEUE";
    public static final int TOTAL_BYTES_REQUIRED_FOR_WRITER_CONTEXT = 8;

    private final int msgLength;
    private final long queueCapacity;
    private final RandomAccessFile queue;
    private final RandomAccessFile queueReaderContext;
    private final MappedByteBuffer[] queueBuffers;
    private final MappedByteBuffer readerContextBuffer;
    private final FileChannel queueChannel;
    private final FileChannel queueReaderContextChannel;
    private final int numberOfMessagesPerBuffer;

    //declared volatile to flush store buffers when queue's used within the JVM
    private volatile long writeIndex;
    private volatile long readIndex;

    private final byte[] lastDequedMsg;
    private final String queuePath;
    private final String queueReaderContextPath;

    private static final Logger LOG = LoggerFactory.getLogger(CircularMMFQueue.class);
    private int indexToAck = -1;

    public CircularMMFQueue(int msgSize, int queueCapacity, String path) throws IOException {

        this.msgLength = msgSize;
        this.queueCapacity = queueCapacity;

        createDirectoryStructureIfDoesntExist(path);

        queuePath = path + "/" + NAME + ".txt";
        queueReaderContextPath = path + "/" + NAME + "-reader-context.txt";
        queue = new RandomAccessFile(queuePath, "rw");
        queueReaderContext = new RandomAccessFile(queueReaderContextPath, "rw");

        queueReaderContextChannel = queueReaderContext.getChannel();
        readerContextBuffer = queueReaderContextChannel.map(READ_WRITE, 0, 8);
        queueChannel = queue.getChannel();
        queueBuffers = initializedBuffers(msgSize, queueCapacity);
        writeIndex = currentWriterIndex();
        readIndex = currentReaderIndex();

        lastDequedMsg = new byte[msgSize];
        numberOfMessagesPerBuffer = Integer.MAX_VALUE / msgSize;

        LOG.info("Queue is setup with size {}, reader is at {}, writer is at {}, queue-size {}", queueCapacity, readIndex, writeIndex, getQueueSize());
    }

    public byte[] get() {

        if (isReaderIndexAheadOfWriters()) return null;
        if (isAMsgAwaitingAck()) return null;
        if (isEmpty()) return null;

        int nextIndexToReadFrom = nextIndexToReadFrom();
        readNextMessage(nextIndexToReadFrom);
        updateReaderContext();
        return lastDequedMsg;
    }


    public byte[] getWithAck() {

        if (isReaderIndexAheadOfWriters()) return null;
        if (isAMsgAwaitingAck()) return null;
        if (isEmpty()) return null;

        int nextIndexToReadFrom = nextIndexToReadFrom();
        indexToAck = nextIndexToReadFrom;
        readNextMessage(nextIndexToReadFrom);
        return lastDequedMsg;
    }

    public boolean add(byte[] msg) {

        if (isReaderIndexAheadOfWriters()) return false;
        if (isFull()) return false;

        int bufferIndex = getBufferIndexOfTheNextReadLocation(writeAtIndex());
        MappedByteBuffer buffer = queueBuffers[bufferIndex];
        buffer.position(getIndexWithinBuffer(writeAtIndex(), bufferIndex));
        buffer.put(msg, 0, msg.length);
        updateWriterContext();
        return true;
    }


    private void readNextMessage(int nextIndexToReadFrom) {
        int bufferIndex = getBufferIndexOfTheNextReadLocation(nextIndexToReadFrom);
        MappedByteBuffer buffer = queueBuffers[bufferIndex];
        buffer.position(getIndexWithinBuffer(nextIndexToReadFrom, bufferIndex));
        buffer.get(lastDequedMsg, 0, msgLength);
    }

    public boolean isFull() {
        return (writeIndex - currentReaderIndex()) >= (queueCapacity);
    }

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

    public void ack() {
        if (indexToAck == nextIndexToReadFrom()) {
            updateReaderContext();
            indexToAck = -1;
        }
    }

    private int nextIndexToReadFrom() {
        return (int) (readIndex % queueCapacity);
    }

    private int writeAtIndex() {
        return (int) (writeIndex % queueCapacity);
    }


    private int getBufferIndexOfTheNextReadLocation(int index) {
        int ret = index / numberOfMessagesPerBuffer;
        if (ret >= queueBuffers.length) {
            throw new IndexOutOfBoundsException("Index doesn't exist");
        }

        return ret;
    }

    private int getIndexWithinBuffer(int index, int bufferIndex) {
        int indexWithinBuffer = (index % numberOfMessagesPerBuffer) * msgLength;
        return bufferIndex == 0 ? indexWithinBuffer + TOTAL_BYTES_REQUIRED_FOR_WRITER_CONTEXT : bufferIndex;
    }


    private void updateReaderContext() {
        readerContextBuffer.putLong(0, readIndex + 1);
        readIndex++; //flush store buffers
    }

    private long currentReaderIndex() {
        return readerContextBuffer.getLong(0);
    }

    private void updateWriterContext() {
        MappedByteBuffer firstQueueBuffer = queueBuffers[0];
        firstQueueBuffer.putLong(0, writeIndex + 1);
        writeIndex++; // flush store buffers
    }

    private long currentWriterIndex() {
        return queueBuffers[0].getLong(0);
    }

    public static CircularMMFQueue getInstance(int msgSize, String path) throws IOException {
        return new CircularMMFQueue(msgSize, DEFAULT_SIZE, path);
    }

    public static CircularMMFQueue getInstance(int msgSize, int queueSize, String path) throws IOException {
        return new CircularMMFQueue(msgSize, queueSize, path);
    }

    public long getQueueSize() {
        return currentWriterIndex() - currentReaderIndex();
    }

    public long messagesWritten() {
        return currentWriterIndex();
    }

    public long messagesRead() {
        return currentReaderIndex();
    }

    private MappedByteBuffer[] initializedBuffers(long msgSize, int queueCapacity) throws IOException {

        long totalBytesRequiredToAccommodateCapacity = msgSize * queueCapacity;
        totalBytesRequiredToAccommodateCapacity += TOTAL_BYTES_REQUIRED_FOR_WRITER_CONTEXT; //first X bytes reserved for writer's sequence
        int numberOfBuffers = (int) Math.ceil((double) totalBytesRequiredToAccommodateCapacity / (double) Integer.MAX_VALUE);
        MappedByteBuffer[] queueBuffers = new MappedByteBuffer[numberOfBuffers];
        for (int i = 0; i < numberOfBuffers; i++) {
            queueBuffers[i] = queueChannel.map(READ_WRITE, (long) i * Integer.MAX_VALUE, Integer.MAX_VALUE);
        }
        return queueBuffers;
    }

    private static void createDirectoryStructureIfDoesntExist(String path) throws IOException {
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException(String.format("Failed to create directory structure %s", path));
        }
    }

    public void reset() {
        stream(this.queueBuffers).forEach(MappedByteBuffer::clear);
        this.readerContextBuffer.putInt(0, 0);
        this.readerContextBuffer.clear();
        this.readIndex = 0;
        this.writeIndex = 0;
        readerContextBuffer.putLong(0, readIndex);
        queueBuffers[0].putLong(0, writeIndex);
    }

    public void cleanup() {
        try {
            closeQueue();
            delete(Path.of(queuePath));
            delete(Path.of(queueReaderContextPath));
        } catch (IOException e) {
            LOG.error("Failed to delete underlying files", e);
        }
    }

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
}