package com.abidi.queue;

public class SPSCVolatileCircularQueue implements SPSCCircularQueue {

    private long p00, p01, p02, p03, p04, p05, p06;
    private volatile long readerIndex;
    private long p10, p11, p12, p13, p14, p15, p16;
    private volatile long writerIndex;
    private long p20, p21, p22, p23, p24, p25, p26;

    private final String[] elements;
    private final int mask;

    public SPSCVolatileCircularQueue(int size) {

        if (Integer.bitCount(size) != 1) {
            throw new IllegalArgumentException("capacity must be a power of two");
        }

        readerIndex = 0;
        writerIndex = 0;
        elements = new String[size];
        mask = size - 1;
    }

    @Override
    public boolean add(String msg) {
        long currentWriterIndex = writerIndex;
        if (currentWriterIndex - readerIndex >= elements.length) {
            return false;
        }
        int index = (int) (currentWriterIndex & mask);
        elements[index] = msg;
        writerIndex++;  // volatile write — acts as release fence
        return true;
    }

    @Override
    public String get() {

        long currentReaderIndex = readerIndex;
        if (currentReaderIndex == writerIndex) {
            return null;
        }

        int index = (int) (currentReaderIndex & mask);
        String msg = elements[index];
        readerIndex++;  // volatile write — acts as release fence
        return msg;
    }

}

