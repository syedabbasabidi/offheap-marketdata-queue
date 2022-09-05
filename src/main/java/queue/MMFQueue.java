package queue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class MMFQueue {

    public static final int QUEUE_SIZE = 100_000_000;

    private final MappedByteBuffer[] buffers;
    private final int objSize;
    private final long queueSize;
    private final RandomAccessFile queue;
    private final RandomAccessFile queueWriterContext;
    private final RandomAccessFile queueReaderContext;
    private final int bufferObjCapacity;
    private final MappedByteBuffer writerContextBuffer;
    private final MappedByteBuffer readerContextBuffer;
    private volatile int writeIndex;
    private volatile int readIndex;

    public MMFQueue(int objSize, int queueSize, String name, boolean redo) throws IOException {

        this.objSize = objSize;
        this.queueSize = queueSize;

        bufferObjCapacity = Integer.MAX_VALUE / objSize;
        long sizeInBytes = (long) objSize * queueSize;
        int numberOfBuffers = (int) Math.ceil((double) sizeInBytes / (double) Integer.MAX_VALUE);
        buffers = new MappedByteBuffer[numberOfBuffers];

        queue = new RandomAccessFile("/tmp/" + name + ".txt", "rw");
        queueWriterContext = new RandomAccessFile("/tmp/" + name + "-writer-context.txt", "rw");
        queueReaderContext = new RandomAccessFile("/tmp/" + name + "-reader-context.txt", "rw");
        writerContextBuffer = queueWriterContext.getChannel().map(READ_WRITE, 0, 4);
        readerContextBuffer = queueReaderContext.getChannel().map(READ_WRITE, 0, 4);
        writeIndex = redo ? 0 : currentWriterIndex();
        readIndex = redo ? 0 : currentReaderIndex();


        FileChannel fileChannel = queue.getChannel();
        for (int i = 0; i < numberOfBuffers; i++) {
            buffers[i] = fileChannel.map(READ_WRITE, (long) i * Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        System.out.println("Queue initialized with size " + queueSize);
    }

    private int getBuffer(int index) {
        int ret = index / bufferObjCapacity;
        if (ret >= buffers.length) {
            throw new IndexOutOfBoundsException("Index doesn't exist");
        }

        return ret;
    }

    private int getIndexWithinBuffer(int index) {
        return (index % bufferObjCapacity) * objSize;
    }

    private byte[] get(int index) {
        MappedByteBuffer buffer = buffers[getBuffer(index)];
        byte[] ret = new byte[objSize];
        buffer.position(getIndexWithinBuffer(index));
        buffer.get(ret, 0, objSize);
        updateReaderContext();
        readIndex++; //flush store buffers
        return ret;
    }


    public Optional<byte[]> get() {
        writeIndex = currentWriterIndex();
        return Optional.ofNullable(hasNext() ? get(readIndex) : null);
    }

    public void add(byte[] object) {

        readIndex = currentReaderIndex();

        if (writeIndex >= queueSize) {
            throw new IllegalStateException("Queue is full");
        }

        MappedByteBuffer buffer = buffers[getBuffer(writeIndex)];
        buffer.position(getIndexWithinBuffer(writeIndex));
        buffer.put(object, 0, object.length);
        updateWriterContext();
        writeIndex++; // flush store buffers

    }

    private int currentReaderIndex() {
        return readerContextBuffer.getInt(0);
    }

    private void updateWriterContext() {
        writerContextBuffer.putInt(0, writeIndex + 1);
    }

    private void updateReaderContext() {
        readerContextBuffer.putInt(0, readIndex + 1);
    }

    private boolean hasNext() {
        return readIndex < writeIndex;
    }

    private int currentWriterIndex() {
        return writerContextBuffer.getInt(0);
    }

    public long messagesWritten() {
        return writeIndex;
    }

    public long messagesRead() {
        return readIndex;
    }

    public static MMFQueue getInstance(int objSize) throws IOException {
        return new MMFQueue(objSize, QUEUE_SIZE, "FAST_QUEUE", true);
    }

    public void shutdown() {
        try {
            queue.close();
            queueWriterContext.close();
        } catch (IOException e) {
            System.out.println("Failed to close underlying file" + e);
        }
    }
}