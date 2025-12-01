package com.abidi.queue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class MPMCLockFreeCircularQueue {

    private static final VarHandle READER_INDEX_VH;
    private static final VarHandle WRITER_INDEX_VH;

    private long p00, p01, p02, p03, p04, p05, p06;
    private long readerIndex;
    private long p10, p11, p12, p13, p14, p15, p16;
    private long writerIndex;
    private long p20, p21, p22, p23, p24, p25, p26;

    private final Slot[] array;
    private final int mask;

    static {

        try {
            READER_INDEX_VH = MethodHandles.lookup().findVarHandle(MPMCLockFreeCircularQueue.class, "readerIndex", long.class);
            WRITER_INDEX_VH = MethodHandles.lookup().findVarHandle(MPMCLockFreeCircularQueue.class, "writerIndex", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public MPMCLockFreeCircularQueue(int capacity) {

        if (Integer.bitCount(capacity) != 1)
            throw new IllegalArgumentException("Must be power of 2");

        array = new Slot[capacity];
        for (int i = 0; i < capacity; i++) {
            array[i] = new Slot("", 0);
        }
        mask = capacity - 1;
    }

    public boolean add(String msg) {

        while (true) {

            long currentIndex = (long) WRITER_INDEX_VH.getOpaque(this);// producerSeqNum.get();
            //queue is full
            if (currentIndex - ((long) READER_INDEX_VH.getOpaque(this)) >= array.length) {
                return false;
            }
            //race to add the msg add currentIndex
            if (WRITER_INDEX_VH.compareAndSet(this, currentIndex, currentIndex + 1)) {
                array[(int) (currentIndex & mask)].msg = msg;
                //volatile write
                array[(int) (currentIndex & mask)].msgNum = currentIndex + 1;
                return true;
            }
        }
    }

    public String get() {

        while (true) {

            long currentIndex = (long) READER_INDEX_VH.getOpaque(this);
            //queue is empty
            if (currentIndex == (long) WRITER_INDEX_VH.getOpaque(this)) {
                return null;
            }
            // is producer seq number is 1+ last read index,
            // + volatile read
            int arrayIndex = (int) (currentIndex & mask);
            if (array[arrayIndex].msgNum != currentIndex + 1) {
                continue;
            }

            //race to read msg from current index
            Slot slot = array[arrayIndex];
            if (READER_INDEX_VH.compareAndSet(this, currentIndex, currentIndex + 1)) {
                return slot.msg;
            }
        }
    }

    private static class Slot {
        private String msg;
        private volatile long msgNum;

        public Slot(String msg, long msgNum) {
            this.msg = msg;
            this.msgNum = msgNum;
        }
    }

    public long size() {
        return (long) WRITER_INDEX_VH.getAcquire(this) - (long) READER_INDEX_VH.getAcquire(this);
    }
}