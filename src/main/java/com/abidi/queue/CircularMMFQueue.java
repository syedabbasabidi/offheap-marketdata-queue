package com.abidi.queue;

import java.io.IOException;

public interface CircularMMFQueue extends AutoCloseable {

    int DEFAULT_SIZE = 100_000;

    boolean add(byte[] msg);

    boolean add(byte[] msg, int offset, int length);

    byte[] get();

    byte[] getWithAck();

    boolean readWithAck(MessageHandler handler);

    void ack();

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
