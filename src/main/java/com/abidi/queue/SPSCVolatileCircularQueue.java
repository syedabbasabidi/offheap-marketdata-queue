package com.abidi.queue;

import com.abidi.marketdata.model.MarketData;
import jdk.internal.vm.annotation.Contended;

public class SPSCVolatileCircularQueue implements SPSCCircularQueue<MarketData> {

    @Contended("readerIndex")  private volatile long readerIndex;
    @Contended("readerIndex") private volatile long writerIndex;
    private static final int SLOT_LONGS = 16;
    private static final int SLOT_SHIFT = 4;

    private final MarketData[] elements;
    private final int mask;
    private final int capacity;

    public SPSCVolatileCircularQueue(int size) {

        if (Integer.bitCount(size) != 1) {
            throw new IllegalArgumentException("capacity must be a power of two");
        }

        readerIndex = 0;
        writerIndex = 0;
        elements = new MarketData[size * SLOT_LONGS];
        mask = size - 1;
        capacity = size;
    }

    @Override
    public boolean add(MarketData msg) {
        long currentWriterIndex = writerIndex;
        if (currentWriterIndex - readerIndex >= capacity) {
            return false;
        }
        int index = (int) (currentWriterIndex & mask);
        elements[index << SLOT_SHIFT] = msg;
        writerIndex++;
        return true;
    }

    @Override
    public MarketData get() {

        long currentReaderIndex = readerIndex;
        if (currentReaderIndex == writerIndex) {
            return null;
        }

        int index = (int) (currentReaderIndex & mask);
        MarketData msg = elements[index << SLOT_SHIFT];
        readerIndex++;
        return msg;
    }

    @Override
    public boolean batchAdd(MarketData[] messages) {

        long currentWriterIndex = writerIndex;
        int remainingCapacity = (int) (capacity - (currentWriterIndex - readerIndex));
        if (messages.length > remainingCapacity) {
            return false;
        }

        for (int i = 0; i < messages.length; i++) {
            int index = (int) ((currentWriterIndex + i) & mask);
            elements[index << SLOT_SHIFT] = messages[i];
        }
        writerIndex += messages.length;
        return true;
    }

    @Override
    public boolean batchGet(MarketData[] buffer) {

        long currentReaderIndex = readerIndex;
        int availableMessages = (int) (writerIndex - currentReaderIndex);
        if (availableMessages == 0) {
            return false;
        }
        int toRead = Math.min(buffer.length, availableMessages);
        for (int i = 0; i < toRead; i++) {
            int index = (int) ((currentReaderIndex + i) & mask);
            buffer[i] = elements[index << SLOT_SHIFT];
        }
        readerIndex += toRead;
        return true;
    }


}

