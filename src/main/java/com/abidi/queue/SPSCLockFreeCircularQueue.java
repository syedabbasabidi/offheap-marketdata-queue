package com.abidi.queue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class SPSCLockFreeCircularQueue implements SPSCCircularQueue {

    private static final VarHandle READER_INDEX_VH;
    private static final VarHandle WRITER_INDEX_VH;

    private long p00, p01, p02, p03, p04, p05, p06;
    private long readerIndex;
    private long p10, p11, p12, p13, p14, p15, p16;
    private long writerIndex;
    private long p20, p21, p22, p23, p24, p25, p26;

    private final String[] elements;
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
        elements = new String[size];
        mask = size - 1;
    }


    public boolean add(String msg) {
        long currentWriterIndex = writerIndex;
        long currentReaderIndex = (long) READER_INDEX_VH.getAcquire(this);
        if (currentWriterIndex - currentReaderIndex >= elements.length) {
            return false;
        }
        int index = (int) (currentWriterIndex & mask);
        elements[index] = msg;
        WRITER_INDEX_VH.setRelease(this, currentWriterIndex + 1);
        return true;
    }

    public String get() {

        long currentReaderIndex = readerIndex;
        long currentWriterIndex = (long) WRITER_INDEX_VH.getAcquire(this);
        if (currentReaderIndex == currentWriterIndex) {
            return null;
        }

        int index = (int) (currentReaderIndex & mask);
        String msg = elements[index];
        READER_INDEX_VH.setRelease(this, currentReaderIndex + 1);
        return msg;
    }

}