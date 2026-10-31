package com.abidi.queue;

import java.io.IOException;

/**
 * Single producer / single consumer queue over memory-mapped files; producer and consumer may live in different processes.
 * <ul>
 *     <li>{@link CircularMMFQueueFixedSize}: every message occupies one slot of the same size.</li>
 *     <li>{@link CircularMMFQueueVariableSize}: every message occupies a header plus its own length, so sizes may differ.</li>
 * </ul>
 */
public interface CircularMMFQueue extends AutoCloseable {

    int DEFAULT_SIZE = 100_000;

    /**
     * @return false if the queue is full
     */
    boolean add(byte[] msg);

    /**
     * @return false if the queue is full
     */
    boolean add(byte[] msg, int offset, int length);

    /**
     * Reads and acknowledges the next message, null if empty.
     */
    byte[] get();

    /**
     * Reads the next message without acknowledging it, null if empty or a message awaits {@link #ack()}.
     */
    byte[] getWithAck();

    /**
     * Zero-copy variant of {@link #getWithAck()}: hands the mapped message to the handler, call {@link #ack()} afterwards.
     *
     * @return false if empty or a message awaits {@link #ack()}
     */
    boolean readWithAck(MessageHandler handler);

    void ack();

    /**
     * @return true if a message of {@link #maxMessageSize()} bytes would be refused
     */
    boolean isFull();

    boolean isEmpty();

    int maxMessageSize();

    long getQueueSize();

    long messagesWritten();

    long messagesRead();

    void reset();

    void cleanup();

    void closeQueue();

    @Override
    void close();

    static CircularMMFQueue getInstance(int msgSize, String path) throws IOException {
        return new CircularMMFQueueFixedSize(msgSize, DEFAULT_SIZE, path);
    }

    static CircularMMFQueue getInstance(int msgSize, int queueSize, String path) throws IOException {
        return new CircularMMFQueueFixedSize(msgSize, queueSize, path);
    }
}
