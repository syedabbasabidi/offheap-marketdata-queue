package com.abidi.consumer;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CircularQueueConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CircularQueueConsumer.class);
    private final ByteUtils byteUtils;

    public CircularQueueConsumer(ByteUtils byteUtils) {
        this.byteUtils = byteUtils;
    }

    public static void main(String[] args) {

        LOG.info("Starting consumer ...");
        CircularQueueConsumer circularQueueConsumer = new CircularQueueConsumer(new ByteUtils());
        circularQueueConsumer.run();
    }

    public void run() {

        MarketDataCons marketData = new MarketDataCons(byteUtils);
        CircularMMFQueue mmfQueue = getInstance(marketData);

        if (mmfQueue == null) {
            throw new RuntimeException("Failed to get instance of CircularMMFQueue");
        }

        LOG.info("Consumer started...");
        while (true) {
            byte[] bytes = mmfQueue.get();
            if (bytes != null) {
                process(marketData, bytes);
            }
        }
    }


    private static void process(MarketDataCons marketData, byte[] data) {
        marketData.setData(data);
        LOG.debug("Message received {}", marketData);
    }

    private static CircularMMFQueue getInstance(MarketDataCons marketData) {
        try {
            return CircularMMFQueue.getInstance(marketData.size(), "/tmp");
        } catch (IOException e) {
            LOG.error("Failed to set up queue", e);
            return null;
        }
    }
}