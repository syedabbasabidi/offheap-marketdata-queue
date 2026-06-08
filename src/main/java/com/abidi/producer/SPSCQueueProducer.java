package com.abidi.producer;

import com.abidi.marketdata.MarketDataFactory;
import com.abidi.marketdata.model.MarketData;
import com.abidi.queue.SPSCCircularQueue;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class SPSCQueueProducer implements Runnable {

    private final SPSCCircularQueue<MarketData> queue;
    private final boolean batchedProducer;
    private final MarketDataFactory marketDataFactory = new MarketDataFactory();
    private static final Logger LOG = LoggerFactory.getLogger(SPSCQueueProducer.class);
    private long messagesProducer = 0;


    public SPSCQueueProducer(SPSCCircularQueue<MarketData> queue) {
        this.queue = queue;
        this.batchedProducer = false;
    }

    public SPSCQueueProducer(SPSCCircularQueue<MarketData> queue, boolean batchedProducer) {
        this.queue = queue;
        this.batchedProducer = batchedProducer;
    }

    @Override
    public void run() {
        Affinity.setAffinity(0);

        if (!batchedProducer) {
            MarketData marketData = marketDataFactory.create();
            LOG.info("Producer started...");
            while (!Thread.currentThread().isInterrupted()) {
                if (!queue.add(marketData)) {
                    Thread.onSpinWait();
                }
                messagesProducer++;
            }
        } else {

            int batchSize = 1000;
            MarketData[] batchedMessages = new MarketData[batchSize];
            Arrays.fill(batchedMessages, marketDataFactory.create());
            LOG.info("Starting producer with batched messages...");
            while (!Thread.currentThread().isInterrupted()) {
                while (!queue.batchAdd(batchedMessages)) {
                    Thread.onSpinWait();
                }
                messagesProducer += batchSize;
            }
        }
        LOG.info("Producer stopped. Total messages produced={}", messagesProducer);

    }
}
