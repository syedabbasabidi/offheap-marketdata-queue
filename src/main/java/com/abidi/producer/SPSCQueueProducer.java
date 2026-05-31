package com.abidi.producer;

import com.abidi.queue.SPSCCircularQueue;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class SPSCQueueProducer implements Runnable {

    private final SPSCCircularQueue queue;
    private final boolean batchedProducer;
    private static final Logger LOG = LoggerFactory.getLogger(SPSCQueueProducer.class);
    private long messagesProducer = 0;


    public SPSCQueueProducer(SPSCCircularQueue queue) {
        this.queue = queue;
        this.batchedProducer = false;
    }

    public SPSCQueueProducer(SPSCCircularQueue queue, boolean batchedProducer) {
        this.queue = queue;
        this.batchedProducer = batchedProducer;
    }

    @Override
    public void run() {
        Affinity.setAffinity(0);

        if (!batchedProducer) {
            LOG.info("Producer started...");
            while (!Thread.currentThread().isInterrupted()) {
                if (!queue.add(System.nanoTime())) {
                    Thread.onSpinWait();
                }
                messagesProducer++;
            }
        } else {

            int batchSize = 1000;
            long[] batchedMessages = new long[batchSize];
            LOG.info("Starting producer with batched messages...");
            while (!Thread.currentThread().isInterrupted()) {
                long l = System.nanoTime();
                Arrays.fill(batchedMessages, l);
                while (!queue.batchAdd(batchedMessages)) {
                    Thread.onSpinWait();
                }
                messagesProducer += batchSize;
            }
        }
        LOG.info("Producer stopped. Total messages produced={}", messagesProducer);

    }
}
