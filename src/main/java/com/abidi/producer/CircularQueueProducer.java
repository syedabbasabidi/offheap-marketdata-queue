package com.abidi.producer;

import com.abidi.marketdata.model.MarketData;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.abidi.queue.CircularMMFQueue.getInstance;

public class CircularQueueProducer {

    public static final int BATCH_SIZE = 10;
    private static final Logger LOG = LoggerFactory.getLogger(CircularQueueProducer.class);

    public static void main(String[] args) throws IOException {

        LOG.info("Starting producer...");
        MarketData md = new MarketData(new ByteUtils());
        CircularMMFQueue mmfQueue = getInstance(md.size(), "/tmp");
        mmfQueue.reset();

        int j = 0;
        md.set("GB00BJLR0J16", 1d + j, 0, true, (byte) 1, "BRC", "2023-02-14:22:10:13", j);
        while (true) {
            md.setPrice(1d + j);
            md.side(j % 2 == 0 ? 0 : 1);
            md.setFirm(j % 2 == 0);
            md.setId(j);
            j = mmfQueue.add(md.getData()) ? j + 1 : j;
            // if (j > 0 && j % BATCH_SIZE == 0) pause(mmfQueue, (j / BATCH_SIZE));

        }
    }

    private static void pause(CircularMMFQueue mmfQueue, int batchNumber) {
        try {
            LOG.info("Wrote batch number {} , size {}", batchNumber, mmfQueue.getQueueSize());
            Thread.sleep(1_000);
        } catch (InterruptedException e) {
            LOG.error("Failed to pause", e);
        }
    }

    private static String toMB(long init) {
        return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
    }
}