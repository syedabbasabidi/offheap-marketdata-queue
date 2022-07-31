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
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;
    private final int bufferObjCapacity;
    private final int maxLength;
    private final MappedByteBuffer contextBuffer;
    private volatile int writeIndex;
    private volatile int readIndex;

    public MMFQueue(int objSize, int queueSize, String name, boolean reread) throws IOException {

        this.objSize = objSize;
        this.queueSize = queueSize;

        bufferObjCapacity = Integer.MAX_VALUE / objSize;
        long sizeInBytes = (long) objSize * queueSize;
        int numberOfBuffers = (int) Math.ceil((double) sizeInBytes / (double) Integer.MAX_VALUE);
        buffers = new MappedByteBuffer[numberOfBuffers];
        maxLength = bufferObjCapacity * numberOfBuffers;

        randomAccessFile = new RandomAccessFile("/tmp/" + name + ".txt", "rw");
        contextBuffer = new RandomAccessFile("/tmp/" + name + "-context.txt", "rw").getChannel().map(READ_WRITE, 0, 8);
        writeIndex = reread ? 0 : contextBuffer.getInt(0);
        readIndex = reread ? 0 : contextBuffer.getInt(4);


        fileChannel = randomAccessFile.getChannel();
        for (int i = 0; i < numberOfBuffers; i++) {
            buffers[i] = fileChannel.map(READ_WRITE, (long) i * Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        System.out.println("Queue initialized with size " + queueSize);
    }

    private int getBuffer(int index) {
        int ret = index / bufferObjCapacity;
        if (ret >= buffers.length) {throw new IndexOutOfBoundsException("Index doesn't exist");}

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
        writeIndex = contextBuffer.getInt(0);
        return Optional.ofNullable(readIndex < writeIndex ? get(readIndex) : null);
    }

    public void add(byte[] object) {

        if (writeIndex >= queueSize) {throw new IllegalStateException("Queue is full");}

        MappedByteBuffer buffer = buffers[getBuffer(writeIndex)];
        buffer.position(getIndexWithinBuffer(writeIndex));
        buffer.put(object, 0, object.length);
        updateWriter();
        writeIndex++; // flush store buffers

    }

    private void updateWriter() {
        contextBuffer.putInt(0, writeIndex + 1);
        readIndex = contextBuffer.getInt(4);
    }

    private void updateReaderContext() {
        contextBuffer.putInt(4, readIndex + 1);
    }

    public long getSize() {
        return writeIndex;
    }

    public static MMFQueue getInstance(int objSize) throws IOException {
        return new MMFQueue(objSize, QUEUE_SIZE, "FAST_QUEUE", true);
    }
}