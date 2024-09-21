package com.abidi.producer;

import com.abidi.marketdata.model.MarketData;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import com.abidi.util.Latin1StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CircularQueueProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CircularQueueProducer.class);

    public static void main(String[] args) {

        LOG.info("Starting producer...");
        MarketData md = new MarketData(new ByteUtils());
        Latin1StringUtil latin1Util = new Latin1StringUtil();
        CircularMMFQueue mmfQueue = getInstance(md);

        int j = 0;
        String gb00BJLR0J16 = latin1Util.getGb00BJLR0J16InLatin1();
        md.set(gb00BJLR0J16, 1d + j, 0, true, (byte) 1, "BRC", "2023-02-14:22:10:13", j);
        while (true) {
            md.setPrice(1d + j);
            md.side(j % 2 == 0 ? 0 : 1);
            md.setFirm(j % 2 == 0);
            md.setId(j);
            j = mmfQueue.add(md.getData()) ? j + 1 : j;
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

    private static CircularMMFQueue getInstance(MarketData marketData) {
        try {
            return CircularMMFQueue.getInstance(marketData.size(), "/tmp");
        } catch (IOException e) {
            LOG.error("Failed to set up queue", e);
            return null;
        }
    }
}