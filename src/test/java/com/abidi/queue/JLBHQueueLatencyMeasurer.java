package com.abidi.queue;

import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.nanoTime;

public class JLBHQueueLatencyMeasurer implements JLBHTask {


    public static final int ITERATIONS = 5_000_000;
    public static final int THROUGHPUT = 1_000_000;
    public static final int RUNS = 10;
    public static final int WARM_UP_ITERATIONS = 10_000;
    private JLBH jlbh;
    private MPMCLockFreeCircularQueue mpmcLockFreeCircularQueue;
    private SPSCLockFreeCircularQueue spscLockFreeCircularQueue;
    private AtomicLong atomicLong = new AtomicLong();
    private ArrayBlockingQueue<String> arrayBlockingQueue = new ArrayBlockingQueue<>(10);
    private SynchronizedBasedCircularQueue synchronizedBasedCircularQueue = new SynchronizedBasedCircularQueue(10);
    Thread consumerThread;


    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS).iterations(ITERATIONS)
                .throughput(THROUGHPUT).runs(RUNS).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHQueueLatencyMeasurer());
        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        this.spscLockFreeCircularQueue = new SPSCLockFreeCircularQueue(1024);
        this.mpmcLockFreeCircularQueue = new MPMCLockFreeCircularQueue(1024);
        Runnable consumer = () -> {
            while (!Thread.interrupted()) {
                mpmcLockFreeCircularQueue.get();
            }
        };
        consumerThread = new Thread(consumer);
        consumerThread.start();
    }

    @Override
    public void run(long startTimeNS) {
        mpmcLockFreeCircularQueue.add("1");
        //arrayBlockingQueue.offer("1");
        jlbh.sampleNanos((nanoTime() - 10) - startTimeNS);
    }

    @Override
    public void complete() {
        consumerThread.interrupt();
    }
}