package com.abidi.producer;

import com.abidi.marketdata.model.MarketData;
import com.abidi.queue.CircularMMFQueue;
import com.abidi.util.ByteUtils;
import com.abidi.util.ChecksumUtil;
import com.abidi.util.Latin1StringUtil;
import net.openhft.affinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CircularQueueProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CircularQueueProducer.class);
    public static final int PIN_TO_CPU = 1;
    public static final String GILT = "GB00BJLR0J16";

    public static void main(String[] args) {

        LOG.info("Starting producer...");

        Affinity.setAffinity(PIN_TO_CPU);
        MarketData md = new MarketData(new ByteUtils(), new ChecksumUtil());
        Latin1StringUtil latin1Util = new Latin1StringUtil();
        CircularMMFQueue mmfQueue = getInstance(md);

        int j = 0;
        String sec = latin1Util.getStringInLatin1(GILT);
        while (true) {
            md.set(sec, 1d + j, j % 2 == 0 ? 0 : 1, true, (byte) 1, "BRC", "2023-02-14:22:10:13", j);
            j = mmfQueue.add(md.getData()) ? j + 1 : j;
        }
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