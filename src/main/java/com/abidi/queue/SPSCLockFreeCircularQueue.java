package com.abidi.queue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import jdk.internal.vm.annotation.Contended;

public class SPSCLockFreeCircularQueue implements SPSCCircularQueue {

    private static final VarHandle READER_INDEX_VH;
    private static final VarHandle WRITER_INDEX_VH;
    @Contended("readerIndex") private long readerIndex;
    @Contended("writerIndex") private long writerIndex;
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
    }


    public boolean add(long msg) {
        long currentWriterIndex = writerIndex;
        long currentReaderIndex = (long) READER_INDEX_VH.getAcquire(this);
        if (currentWriterIndex - currentReaderIndex >= elements.length) {
            return false;
        }
        int index = (int) (currentWriterIndex & mask);
        elements[index << SLOT_SHIFT] = msg;
        WRITER_INDEX_VH.setRelease(this, currentWriterIndex + 1);
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