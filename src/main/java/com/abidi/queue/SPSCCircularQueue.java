package com.abidi.queue;

public interface SPSCCircularQueue {

    boolean add(long msg);

    long get();

    boolean batchAdd(long[] messages);

    boolean batchGet(long[] buffer);

}
