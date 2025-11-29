package com.abidi.queue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class SPSCLockFreeCircularQueue {

    private static final VarHandle READER_INDEX_VH;
    private static final VarHandle WRITER_INDEX_VH;

    private int readerIndex;
    private int writerIndex;
    private final String elements[];

    static {

        try {
            READER_INDEX_VH = MethodHandles.lookup().findVarHandle(SPSCLockFreeCircularQueue.class, "readerIndex", int.class);
            WRITER_INDEX_VH = MethodHandles.lookup().findVarHandle(SPSCLockFreeCircularQueue.class, "writerIndex", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public SPSCLockFreeCircularQueue(int size) {
        readerIndex = 0;
        writerIndex = 0;
        elements = new String[size];
    }


    public void add(String msg) {
        if (isFull()) {
            return;
        }
        elements[writerIndex % elements.length] = msg;
        WRITER_INDEX_VH.setRelease(this, ++writerIndex);
    }
    public boolean isFull() {
        return writerIndex - (int) READER_INDEX_VH.getOpaque(this) >= elements.length;
    }

    public String remove() {

        if (isEmpty()) {
            return null;
        }
        String msg = elements[readerIndex % elements.length];
        READER_INDEX_VH.setOpaque(this, ++readerIndex);
        return msg;
    }

    public boolean isEmpty() {
        return readerIndex == (int) WRITER_INDEX_VH.getAcquire(this);
    }

}
