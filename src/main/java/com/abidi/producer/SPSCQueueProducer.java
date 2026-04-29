package com.abidi.producer;

import com.abidi.queue.SPSCCircularQueue;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPSCQueueProducer implements Runnable {

    private final SPSCCircularQueue queue;
    private static final Logger LOG = LoggerFactory.getLogger(SPSCQueueProducer.class);


    public SPSCQueueProducer(SPSCCircularQueue queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        Affinity.setAffinity(0);

        LOG.info("Producer started...");
        long sentAt = System.nanoTime();
        String time = Long.toString(sentAt);
        while (true) {
            if (!queue.add(time)) {
                Thread.onSpinWait();
            }
        }

    }
}

