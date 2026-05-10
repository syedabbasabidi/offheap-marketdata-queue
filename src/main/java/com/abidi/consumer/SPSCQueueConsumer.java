package com.abidi.consumer;

import com.abidi.queue.SPSCCircularQueue;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SPSCQueueConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SPSCQueueConsumer.class);
    private final SPSCCircularQueue queue;
    private long messagesConsumed = 0;

    public SPSCQueueConsumer(SPSCCircularQueue queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
           Affinity.setAffinity(2);

        LOG.info("Consumer started...");
        while (!Thread.currentThread().isInterrupted()) {
            if (queue.get() == -1) {
                Thread.onSpinWait();
            }else {
                messagesConsumed++;
            }
        }
        LOG.info("Consumer stopped. Total messages consumed={}", messagesConsumed);
    }

}
