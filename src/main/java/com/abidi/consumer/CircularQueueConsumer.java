package com.abidi.consumer;

import com.abidi.marketdata.model.MarketDataCons;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.lang.Thread.interrupted;

public class CircularQueueConsumer implements Runnable {

    private final int howManyToConsume;
    private static final Logger LOG = LoggerFactory.getLogger(CircularQueueConsumer.class);
    private final ByteUtils byteUtils;

    public CircularQueueConsumer(int howManyToConsumer) {
        byteUtils = new ByteUtils();
        this.howManyToConsume = howManyToConsumer == -1 ? parseInt(getProperty("concount", "-1")) : -1;
    }

    public static void main(String[] args) {

        LOG.info("Starting consumer ...");
        CircularQueueConsumer circularQueueConsumer = new CircularQueueConsumer(args.length > 0 ? parseInt(args[0]) : -1);
        circularQueueConsumer.run();
    }

    private boolean hasConsumedEnough(int totalConsumedMessages) {
        if (howManyToConsume == -1) return false;
        return howManyToConsume <= totalConsumedMessages;
    }

    public void run() {

        MarketDataCons marketData = new MarketDataCons(byteUtils);
        CircularMMFQueue mmfQueue = getInstance(marketData);
        LOG.info("Reading to consume");
        int totalConsumedMessages = 0;
        while (true) {

            if (interrupted() || hasConsumedEnough(totalConsumedMessages)) break;

            byte[] bytes = mmfQueue.get();
            if (bytes != null) {
                process(marketData, bytes);
                totalConsumedMessages++;
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