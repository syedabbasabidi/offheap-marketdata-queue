package com.abidi.consumer;

import com.abidi.queue.SPSCCircularQueue;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPSCQueueConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SPSCQueueConsumer.class);

    private final SPSCCircularQueue queue;

    public SPSCQueueConsumer(SPSCCircularQueue queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        Affinity.setAffinity(2);

        LOG.info("Consumer started...");
        while (true) {
            if (queue.get() == null) {
                Thread.onSpinWait();
            }
        }
    }

}

