package com.abidi.producer;

import com.abidi.queue.SPSCCircularQueue;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPSCQueueProducer implements Runnable {

    private final SPSCCircularQueue queue;
    private static final Logger LOG = LoggerFactory.getLogger(SPSCQueueProducer.class);
    private long messagesProducer = 0;


    public SPSCQueueProducer(SPSCCircularQueue queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        Affinity.setAffinity(0);

        LOG.info("Producer started...");
        while (!Thread.currentThread().isInterrupted()) {
            if (!queue.add(System.nanoTime())) {
                Thread.onSpinWait();
            }
            messagesProducer++;
        }
        LOG.info("Producer stopped. Total messages produced={}", messagesProducer);
    }
}
