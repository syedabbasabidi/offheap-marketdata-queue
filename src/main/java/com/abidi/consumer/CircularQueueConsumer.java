package com.abidi.consumer;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class CircularQueueConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CircularQueueConsumer.class);
    public static final int PIN_TO_CPU = 0;
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
        Optional<CircularMMFQueue> mmfQueueOp = getInstance(marketData);

        if (mmfQueueOp.isEmpty()) {
            throw new RuntimeException("Failed to get instance of CircularMMFQueue");
        }

        Affinity.setAffinity(PIN_TO_CPU);
        LOG.info("Consumer started...");

        CircularMMFQueue mmfQueue = mmfQueueOp.get();
        while (true) {
            byte[] bytes = mmfQueue.get();
            if (bytes != null) {
                marketData.setData(bytes);
                LOG.debug("Message received {}", marketData);
            }
        }
    }

    private static Optional<CircularMMFQueue> getInstance(MarketDataCons marketData) {
        try {
            return Optional.of(CircularMMFQueue.getInstance(marketData.size(), "/tmp"));
        } catch (IOException e) {
            LOG.error("Failed to set up queue", e);
            return Optional.empty();
        }
    }
}