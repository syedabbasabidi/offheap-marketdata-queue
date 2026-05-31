package com.abidi.consumer;

import com.abidi.queue.SPSCCircularQueue;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPSCQueueConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SPSCQueueConsumer.class);
    private final SPSCCircularQueue queue;
    private long messagesConsumed = 0;
    private final boolean batchedConsumer;

    public SPSCQueueConsumer(SPSCCircularQueue queue, boolean batchedConsumer) {
        this.queue = queue;
        this.batchedConsumer = batchedConsumer;
    }

    public SPSCQueueConsumer(SPSCCircularQueue queue) {
        this.queue = queue;
        this.batchedConsumer = false;
    }

    @Override
    public void run() {

        Affinity.setAffinity(2);

        if (batchedConsumer) {
            int batchSize = 1000;
            long[] batchBuffer = new long[batchSize];
            LOG.info("Starting consumer with batched messages...");
            while (!Thread.currentThread().isInterrupted()) {
                if (!queue.batchGet(batchBuffer)) {
                    Thread.onSpinWait();
                } else {
                    messagesConsumed += batchSize;
                }
            }
        } else {
            LOG.info("Consumer started...");
            while (!Thread.currentThread().isInterrupted()) {
                if (queue.get() == -1) {
                    Thread.onSpinWait();
                } else {
                    messagesConsumed++;
                }
            }
        }
        LOG.info("Consumer stopped. Total messages consumed={}", messagesConsumed);
    }

}
