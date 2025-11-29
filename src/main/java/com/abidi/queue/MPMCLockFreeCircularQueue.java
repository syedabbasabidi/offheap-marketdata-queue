package com.abidi.queue;

import java.util.concurrent.atomic.AtomicLong;

public class MPMCLockFreeCircularQueue {

    private final Slot[] array;
    private final AtomicLong producerSeqNum = new AtomicLong(0);
    private final AtomicLong consumerSeqNum = new AtomicLong(0);
    private final int sizeAnd;

    public MPMCLockFreeCircularQueue(int size) {

        if (Integer.bitCount(size) != 1)
            throw new IllegalArgumentException("Must be power of 2");

        array = new Slot[size];
        for (int i = 0; i < size; i++) {
            array[i] = new Slot("", 0);
        }
        sizeAnd = size - 1;
    }

    public boolean add(String msg) {

        while (true) {

            long currentIndex = producerSeqNum.get();
            if (currentIndex - consumerSeqNum.get() >= array.length) {
                return false;
            }

            if (producerSeqNum.compareAndSet(currentIndex, currentIndex + 1)) {
                array[(int) (currentIndex & sizeAnd)].msg = msg;
                array[(int) (currentIndex & sizeAnd)].msgNum = currentIndex + 1;
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

            if (array[(int) currentIndex & sizeAnd].msgNum != currentIndex + 1) {
                continue;
            }

            Slot slot = array[(int) (currentIndex & sizeAnd)];
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