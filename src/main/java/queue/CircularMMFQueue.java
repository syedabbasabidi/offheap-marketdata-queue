package queue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class CircularMMFQueue {
    public static final int DEFAULT_SIZE = 10;
    private static final String NAME = "FAST_QUEUE";

    private final MappedByteBuffer[] buffers;
    private final int objSize;
    private final long queueCapacity;
    private final RandomAccessFile queue;
    private final RandomAccessFile queueWriterContext;
    private final RandomAccessFile queueReaderContext;
    private final int bufferObjCapacity;
    private final MappedByteBuffer writerContextBuffer;
    private final MappedByteBuffer readerContextBuffer;
    private final FileChannel queueChannel;
    private final FileChannel queueWriterContextChannel;
    private volatile int writeIndex;
    private volatile int readIndex;

    private int queueSize;
    private byte[] dequedMD;

    private final String queuePath;
    private final String queueWriterContextPath;
    private final String queueReaderContextPath;

    public CircularMMFQueue(int objSize, int queueCapacity, String path) throws IOException {

        this.objSize = objSize;
        this.queueCapacity = queueCapacity;

        queuePath = path + "/" + NAME + ".txt";
        queueReaderContextPath = path + "/" + NAME + "-reader-context.txt";
        queueWriterContextPath = path + "/" + NAME + "-writer-context.txt";

        bufferObjCapacity = Integer.MAX_VALUE / objSize;
        long sizeInBytes = (long) objSize * queueCapacity;
        int numberOfBuffers = (int) Math.ceil((double) sizeInBytes / (double) Integer.MAX_VALUE);
        buffers = new MappedByteBuffer[numberOfBuffers];
        queue = new RandomAccessFile(queuePath, "rw");
        queueWriterContext = new RandomAccessFile(queueWriterContextPath, "rw");
        queueReaderContext = new RandomAccessFile(queueReaderContextPath, "rw");
        queueWriterContextChannel = queueWriterContext.getChannel();
        writerContextBuffer = queueWriterContextChannel.map(READ_WRITE, 0, 8);
        readerContextBuffer = queueReaderContext.getChannel().map(READ_WRITE, 0, 8);
        writeIndex = currentWriterIndex();
        readIndex = currentReaderIndex();


        queueChannel = queue.getChannel();

        for (int i = 0; i < numberOfBuffers; i++) {
            buffers[i] = queueChannel.map(READ_WRITE, (long) i * Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        dequedMD = new byte[objSize];
        System.out.println("Queue initialized with size " + queueCapacity);
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
        buffer.position(getIndexWithinBuffer(index));
        buffer.get(dequedMD, 0, objSize);
        updateReaderContext();
        readIndex++; //flush store buffers
        return dequedMD;
    }


    public Optional<byte[]> get() {

        if (isInResetMode()) {
            readerContextBuffer.putInt(0, 0);
            readerContextBuffer.putInt(4, 1);
            readIndex = 0;
            return Optional.empty();
        }
        readerContextBuffer.putInt(4, 0);

        writeIndex = currentWriterIndex();
        if (!hasNext()) {
            //   System.out.println("Queue is empty");
            return Optional.empty();
        }
        return Optional.of(get(readIndex));
    }

    public boolean add(byte[] object) {

        if (isInResetMode()) {
            if (hasReaderReset()) {
                writerContextBuffer.putInt(0, 0);
                writerContextBuffer.putInt(4, 0);
                writeIndex = 0;
            }
            return false;
        }

        if (writeIndex >= queueCapacity && currentReaderIndex() >= queueCapacity && !hasReaderReset()) {
            tellReaderToReset(); // go in reset-mode
            return false;
        }

        if (writeIndex >= queueCapacity) {
            return false;
        }

        MappedByteBuffer buffer = buffers[getBuffer(writeIndex)];
        buffer.position(getIndexWithinBuffer(writeIndex));
        buffer.put(object, 0, object.length);
        updateWriterContext();
        writeIndex++; // flush store buffers
        queueSize++;
        return true;
    }

    void tellReaderToReset() {
        //System.out.println("Asking Reader to reset index");
        writerContextBuffer.putInt(4, 1);
    }

    boolean hasReaderReset() {
        return readerContextBuffer.getInt(4) == 1;
    }

    boolean isInResetMode() {
        return writerContextBuffer.getInt(4) == 1;
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

    public static CircularMMFQueue getInstance(int objSize, int size, String path) throws IOException {
        return new CircularMMFQueue(objSize, size, path);
    }

    public int getQueueSize() {
        return writeIndex;
    }

    public void reset() {
        try {
            queueChannel.close();
            queueWriterContext.close();
            queueReaderContext.close();
            queue.close();
            queueWriterContext.close();
            queueReaderContext.close();
            Files.delete(Path.of(queuePath));
            Files.delete(Path.of(queueReaderContextPath));
            Files.delete(Path.of(queueWriterContextPath));
        } catch (IOException e) {
            System.out.println("Failed to close underlying file" + e);
        }
    }
}