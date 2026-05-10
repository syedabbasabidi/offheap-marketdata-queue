package com.abidi.queue;

import jdk.internal.vm.annotation.Contended;

public class SPSCVolatileCircularQueue implements SPSCCircularQueue {

    @Contended("readerIndex")  private volatile long readerIndex;
    @Contended("readerIndex") private volatile long writerIndex;
    private static final int SLOT_LONGS = 16;
    private static final int SLOT_SHIFT = 4;

    private final long[] elements;
    private final int mask;

    public SPSCVolatileCircularQueue(int size) {

        if (Integer.bitCount(size) != 1) {
            throw new IllegalArgumentException("capacity must be a power of two");
        }

        readerIndex = 0;
        writerIndex = 0;
        elements = new long[size * SLOT_LONGS];
        mask = size - 1;
    }

    @Override
    public boolean add(long msg) {
        long currentWriterIndex = writerIndex;
        if (currentWriterIndex - readerIndex >= elements.length) {
            return false;
        }
        int index = (int) (currentWriterIndex & mask);
        elements[index << SLOT_SHIFT] = msg;
        writerIndex++;
        return true;
    }

    @Override
    public long get() {

        long currentReaderIndex = readerIndex;
        if (currentReaderIndex == writerIndex) {
            return -1;
        }

        int index = (int) (currentReaderIndex & mask);
        long msg = elements[index << SLOT_SHIFT];
        readerIndex++;
        return msg;
    }

}

