package com.abidi.queue;

import jdk.internal.vm.annotation.Contended;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class SPSCLockFreeCircularQueue implements SPSCCircularQueue {

    private static final VarHandle READER_INDEX_VH;
    private static final VarHandle WRITER_INDEX_VH;
    @Contended("readerIndex")
    private long readerIndex;
    @Contended("writerIndex")
    private long writerIndex;
    private final int capacity;
    private static final int SLOT_LONGS = 16;
    private static final int SLOT_SHIFT = 4;


    private final long[] elements;
    private final int mask;

    static {

        try {
            READER_INDEX_VH = MethodHandles.lookup().findVarHandle(SPSCLockFreeCircularQueue.class, "readerIndex", long.class);
            WRITER_INDEX_VH = MethodHandles.lookup().findVarHandle(SPSCLockFreeCircularQueue.class, "writerIndex", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public SPSCLockFreeCircularQueue(int size) {

        if (Integer.bitCount(size) != 1) {
            throw new IllegalArgumentException("capacity must be a power of two");
        }

        readerIndex = 0;
        writerIndex = 0;
        elements = new long[size * SLOT_LONGS];
        mask = size - 1;
        capacity = size;
    }


    public boolean batchAdd(long[] messages) {

        long currentWriterIndex = writerIndex;
        long currentReaderIndex = (long) READER_INDEX_VH.getAcquire(this);

        int remainingCapacity = (int) (capacity - (currentWriterIndex - currentReaderIndex));
        if (messages.length > remainingCapacity) {
            return false;
        }

        for (int i = 0; i < messages.length; i++) {
            int index = (int) ((currentWriterIndex + i) & mask);
            elements[index << SLOT_SHIFT] = messages[i];
        }
        WRITER_INDEX_VH.setRelease(this, currentWriterIndex + messages.length);
        return true;

    }

    public boolean add(long msg) {
        long currentWriterIndex = writerIndex;
        long currentReaderIndex = (long) READER_INDEX_VH.getAcquire(this);
        if (currentWriterIndex - currentReaderIndex >= capacity) {
            return false;
        }
        int index = (int) (currentWriterIndex & mask);
        elements[index << SLOT_SHIFT] = msg;
        WRITER_INDEX_VH.setRelease(this, currentWriterIndex + 1);
        return true;
    }

    public boolean batchGet(long[] buffer) {

        long currentReaderIndex = readerIndex;
        long currentWriterIndex = (long) WRITER_INDEX_VH.getAcquire(this);
        int availableMessages = (int) (currentWriterIndex - currentReaderIndex);
        if (availableMessages == 0) {
            return false;
        }
        int toRead = Math.min(buffer.length, availableMessages);
        for (int i = 0; i < toRead; i++) {
            int index = (int) ((currentReaderIndex + i) & mask);
            buffer[i] = elements[index << SLOT_SHIFT];
        }
        READER_INDEX_VH.setRelease(this, currentReaderIndex + toRead);
        return true;
    }

    public long get() {

        long currentReaderIndex = readerIndex;
        long currentWriterIndex = (long) WRITER_INDEX_VH.getAcquire(this);
        if (currentReaderIndex == currentWriterIndex) {
            return -1;
        }

        int index = (int) (currentReaderIndex & mask);
        long msg = elements[index << SLOT_SHIFT];
        READER_INDEX_VH.setRelease(this, currentReaderIndex + 1);
        return msg;
    }

}