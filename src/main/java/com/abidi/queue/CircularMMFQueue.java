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

public class CircularMMFQueue {

    public static final int DEFAULT_SIZE = 100_000;
    private static final String NAME = "OFF_HEAP_QUEUE";

    private final int msgLength;
    private final long queueCapacity;
    private final RandomAccessFile queue;
    private final RandomAccessFile queueWriterContext;
    private final RandomAccessFile queueReaderContext;
    private final MappedByteBuffer[] queueBuffers;
    private final MappedByteBuffer writerContextBuffer;
    private final MappedByteBuffer readerContextBuffer;
    private final FileChannel queueChannel;
    private final FileChannel queueWriterContextChannel;
    private final FileChannel queueReaderContextChannel;
    private final int numberOfMessagesPerBuffer;
    private volatile long writeIndex;
    private volatile long readIndex;
    private final byte[] lastDequedMsg;
    private final String queuePath;
    private final String queueWriterContextPath;
    private final String queueReaderContextPath;

    private static final Logger LOG = LoggerFactory.getLogger(CircularMMFQueue.class);
    private int indexToAck = -1;

    public CircularMMFQueue(int msgSize, int queueCapacity, String path) throws IOException {

        this.msgLength = msgSize;
        this.queueCapacity = queueCapacity;

        createDirectoryStructureIfDoesntExist(path);

        queuePath = path + "/" + NAME + ".txt";
        queueReaderContextPath = path + "/" + NAME + "-reader-context.txt";
        queueWriterContextPath = path + "/" + NAME + "-writer-context.txt";
        queue = new RandomAccessFile(queuePath, "rw");
        queueWriterContext = new RandomAccessFile(queueWriterContextPath, "rw");
        queueReaderContext = new RandomAccessFile(queueReaderContextPath, "rw");

        queueWriterContextChannel = queueWriterContext.getChannel();
        queueReaderContextChannel = queueReaderContext.getChannel();
        writerContextBuffer = queueWriterContextChannel.map(READ_WRITE, 0, 8);
        readerContextBuffer = queueReaderContextChannel.map(READ_WRITE, 0, 8);
        queueChannel = queue.getChannel();
        writeIndex = currentWriterIndex();
        readIndex = currentReaderIndex();

        queueBuffers = initializedBuffers(msgSize, queueCapacity);
        lastDequedMsg = new byte[msgSize];
        numberOfMessagesPerBuffer = Integer.MAX_VALUE / msgSize;

        LOG.info("Queue is setup with size {}, reader is at {}, writer is at {}, queue-size {}", queueCapacity, readIndex, writeIndex, getQueueSize());
    }

    public byte[] get() {

        if (isReaderIndexHeadOfWriter()) return null;

        if (isAMsgAwaitingAck()) return null;

        if (isEmpty()) {
         //   LOG.debug("Queue is empty");
            return null;
        }
        int index = readFromIndex();
        MappedByteBuffer buffer = queueBuffers[getBufferIndex(index)];
        buffer.position(getIndexWithinBuffer(index));
        buffer.get(lastDequedMsg, 0, msgLength);
        updateReaderContext();
        return lastDequedMsg;
    }

    public boolean add(byte[] msg) {

        if (isReaderIndexHeadOfWriter()) return false;

        if (isFull()) {
            LOG.debug("Queue is full, cannot add. Queue Size {}, Queue Capacity {}", getQueueSize(), this.queueCapacity);
            return false;
        }

        MappedByteBuffer buffer = queueBuffers[getBufferIndex(writeAtIndex())];
        buffer.position(getIndexWithinBuffer(writeAtIndex()));
        buffer.put(msg, 0, msg.length);
        updateWriterContext();
        buffer.force();
        return true;
    }

    public boolean isFull() {
        return (writeIndex - currentReaderIndex()) >= (queueCapacity);
    }

    private boolean isReaderIndexHeadOfWriter() {
        if (currentReaderIndex() > currentWriterIndex()) {
            LOG.error("Queue is invalid state, reader seems to have read more messages than written");
            return true;
        }
        return false;
    }

    public byte[] getWithoutAck() {

        if (isReaderIndexHeadOfWriter()) return null;

        if (isAMsgAwaitingAck()) return null;

        if (isEmpty()) {
            LOG.debug("Queue is empty");
            return null;
        }
        int index = readFromIndex();
        indexToAck = index;
        MappedByteBuffer buffer = queueBuffers[getBufferIndex(index)];
        buffer.position(getIndexWithinBuffer(index));
        buffer.get(lastDequedMsg, 0, msgLength);
        return lastDequedMsg;
    }

    private boolean isAMsgAwaitingAck() {
        if (indexToAck != -1) {
            LOG.info("Index {} hasn't been acked yet", indexToAck);
            return true;
        }
        return false;
    }

    public void ack() {
        if (indexToAck == readFromIndex()) {
            updateReaderContext();
            indexToAck = -1;
        }
    }

    private int readFromIndex() {
        return (int) (readIndex % queueCapacity);
    }

    private int writeAtIndex() {
        return (int) (writeIndex % queueCapacity);
    }


    private int getBufferIndex(int index) {
        int ret = index / numberOfMessagesPerBuffer;
        if (ret >= queueBuffers.length) {
            throw new IndexOutOfBoundsException("Index doesn't exist");
        }

        return ret;
    }

    private int getIndexWithinBuffer(int index) {
        return (index % numberOfMessagesPerBuffer) * msgLength;
    }


    private void updateReaderContext() {
        readerContextBuffer.putLong(0, readIndex + 1);
        readIndex++; //flush store buffers
        readerContextBuffer.force();
    }

    private long currentReaderIndex() {
        return readerContextBuffer.getLong(0);
    }

    private void updateWriterContext() {
        writerContextBuffer.putLong(0, writeIndex + 1);
        writeIndex++; // flush store buffers
        writerContextBuffer.force();
    }

    public boolean isEmpty() {
        return readIndex >= currentWriterIndex();
    }

    private long currentWriterIndex() {
        return writerContextBuffer.getLong(0);
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
        final MappedByteBuffer[] queueBuffers;
        long totalBytesRequiredToAccommodateCapacity = msgSize * queueCapacity;
        int numberOfBuffers = (int) Math.ceil((double) totalBytesRequiredToAccommodateCapacity / (double) Integer.MAX_VALUE);
        queueBuffers = new MappedByteBuffer[numberOfBuffers];
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
        this.writerContextBuffer.putInt(0, 0);
        this.readerContextBuffer.clear();
        this.writerContextBuffer.clear();
        this.readIndex = 0;
        this.writeIndex = 0;
        readerContextBuffer.putLong(0, readIndex);
        writerContextBuffer.putLong(0, writeIndex);

    }

    public void cleanup() {
        try {
            closeQueue();
            delete(Path.of(queuePath));
            delete(Path.of(queueReaderContextPath));
            delete(Path.of(queueWriterContextPath));
        } catch (IOException e) {
            LOG.error("Failed to delete underlying files", e);
        }
    }

    public void closeQueue() {
        try {
            queueChannel.close();
            queueReaderContextChannel.close();
            queueWriterContextChannel.close();
            queue.close();
            queueWriterContext.close();
            queueReaderContext.close();
        } catch (IOException e) {
            LOG.error("Failed to close underlying file", e);
        }
    }
}