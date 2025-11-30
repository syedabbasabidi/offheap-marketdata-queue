package com.abidi.queue;

import java.util.concurrent.atomic.AtomicLong;

public class MPMCLockFreeCircularQueue {

    private final Slot[] array;
    private long p00, p01, p02, p03, p04, p05, p06;
    private final AtomicLong producerSeqNum = new AtomicLong(0);
    private long p10, p11, p12, p13, p14, p15, p16;
    private final AtomicLong consumerSeqNum = new AtomicLong(0);
    private long p20, p21, p22, p23, p24, p25, p26;

    private final int mask;

    public MPMCLockFreeCircularQueue(int size) {

        if (Integer.bitCount(size) != 1)
            throw new IllegalArgumentException("Must be power of 2");

        array = new Slot[size];
        for (int i = 0; i < size; i++) {
            array[i] = new Slot("", 0);
        }
        mask = size - 1;
    }

    public boolean add(String msg) {

        while (true) {

            long currentIndex = producerSeqNum.get();
            if (currentIndex - consumerSeqNum.get() >= array.length) {
                return false;
            }

            if (producerSeqNum.compareAndSet(currentIndex, currentIndex + 1)) {
                array[(int) (currentIndex & mask)].msg = msg;
                array[(int) (currentIndex & mask)].msgNum = currentIndex + 1;
                return true;
            }
        }
    }

    public String get() {

        while (true) {

            long currentIndex = consumerSeqNum.get();
            if (currentIndex == producerSeqNum.get()) {
                return null;
            }

            if (array[(int) currentIndex & mask].msgNum != currentIndex + 1) {
                continue;
            }

            Slot slot = array[(int) (currentIndex & mask)];
            if (consumerSeqNum.compareAndSet(currentIndex, currentIndex + 1)) {
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
        return producerSeqNum.get() - consumerSeqNum.get();
    }
}