package com.abidi.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String NAME = "FAST_QUEUE";

    private final int objSize;
    private final long queueCapacity;
    private final RandomAccessFile queue;
    private final RandomAccessFile queueWriterContext;
    private final RandomAccessFile queueReaderContext;
    private final MappedByteBuffer[] queueBuffers;
    private final MappedByteBuffer writerContextBuffer;
    private final MappedByteBuffer readerContextBuffer;
    private final FileChannel queueChannel;
    private final FileChannel queueWriterContextChannel;
    private final int bufferObjCapacity;
    private final FileChannel queueReaderContextChannel;
    private volatile long writeIndex;
    private volatile long readIndex;
    private byte[] dequedMD;
    private final String queuePath;
    private final String queueWriterContextPath;
    private final String queueReaderContextPath;

    private static final Logger LOG = LoggerFactory.getLogger(CircularMMFQueue.class);

    public CircularMMFQueue(int objSize, int queueCapacity, String path) throws IOException {

        this.objSize = objSize;
        this.queueCapacity = queueCapacity;

        queuePath = path + "/" + NAME + ".txt";
        queueReaderContextPath = path + "/" + NAME + "-reader-context.txt";
        queueWriterContextPath = path + "/" + NAME + "-writer-context.txt";

        bufferObjCapacity = Integer.MAX_VALUE / objSize;
        long sizeInBytes = (long) objSize * queueCapacity;
        int numberOfBuffers = (int) Math.ceil((double) sizeInBytes / (double) Integer.MAX_VALUE);
        queueBuffers = new MappedByteBuffer[numberOfBuffers];
        queue = new RandomAccessFile(queuePath, "rw");
        queueWriterContext = new RandomAccessFile(queueWriterContextPath, "rw");
        queueReaderContext = new RandomAccessFile(queueReaderContextPath, "rw");
        queueWriterContextChannel = queueWriterContext.getChannel();
        writerContextBuffer = queueWriterContextChannel.map(READ_WRITE, 0, 8);
        queueReaderContextChannel = queueReaderContext.getChannel();
        readerContextBuffer = queueReaderContextChannel.map(READ_WRITE, 0, 8);
        writeIndex = currentWriterIndex();
        readIndex = currentReaderIndex();


        queueChannel = queue.getChannel();

        for (int i = 0; i < numberOfBuffers; i++) {
            queueBuffers[i] = queueChannel.map(READ_WRITE, (long) i * Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        dequedMD = new byte[objSize];
        LOG.info("Queue is setup with size {}", queueCapacity);
    }


    public byte[] get() {

        writeIndex = currentWriterIndex();
        if (!hasNext()) return null;
        return get(readFromIndex());
    }

    public boolean add(byte[] object) {

        if ((writeIndex - currentReaderIndex()) >= (queueCapacity)) return false;

        MappedByteBuffer buffer = queueBuffers[getBuffer(writeAtIndex())];
        buffer.position(getIndexWithinBuffer(writeAtIndex()));
        buffer.put(object, 0, object.length);
        updateWriterContext();
        writeIndex++; // flush store buffers
        return true;
    }

    private int readFromIndex() {
        return (int) (readIndex % queueCapacity);
    }

    private int writeAtIndex() {
        return (int) (writeIndex % queueCapacity);
    }


    private int getBuffer(int index) {
        int ret = index / bufferObjCapacity;
        if (ret >= queueBuffers.length) {
            throw new IndexOutOfBoundsException("Index doesn't exist");
        }

        return ret;
    }

    private int getIndexWithinBuffer(int index) {
        return (index % bufferObjCapacity) * objSize;
    }

    private byte[] get(int index) {

        MappedByteBuffer buffer = queueBuffers[getBuffer(index)];
        buffer.position(getIndexWithinBuffer(index));
        buffer.get(dequedMD, 0, objSize);
        updateReaderContext();
        readIndex++; //flush store buffers
        return dequedMD;
    }

    private long currentReaderIndex() {
        return readerContextBuffer.getLong(0);
    }

    private void updateWriterContext() {
        writerContextBuffer.putLong(0, writeIndex + 1);
    }

    private void updateReaderContext() {
        readerContextBuffer.putLong(0, readIndex + 1);
    }

    private boolean hasNext() {
        return readIndex < writeIndex;
    }

    private long currentWriterIndex() {
        return writerContextBuffer.getLong(0);
    }

    public static CircularMMFQueue getInstance(int objSize, String path) throws IOException {
        return new CircularMMFQueue(objSize, DEFAULT_SIZE, path);
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


    public void flush() {
        try {
            this.queueChannel.force(true);
            queueReaderContextChannel.force(true);
            queueWriterContextChannel.force(true);
        } catch (Exception exp) {
            LOG.error("Exception flushing queue", exp);
        }
    }

    public void cleanup() {
        try {
            queueChannel.close();
            queueReaderContextChannel.close();
            queueWriterContextChannel.close();

            queueWriterContext.close();
            queueReaderContext.close();
            queue.close();

            delete(Path.of(queuePath));
            delete(Path.of(queueReaderContextPath));
            delete(Path.of(queueWriterContextPath));
        } catch (IOException e) {
            LOG.error("Failed to close underlying file", e);
        }
    }
}