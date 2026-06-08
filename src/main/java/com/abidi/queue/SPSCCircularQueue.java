package com.abidi.queue;

public interface SPSCCircularQueue<T> {

    boolean add(T msg);

    T get();

    boolean batchAdd(T[] messages);

    boolean batchGet(T[] buffer);

}
