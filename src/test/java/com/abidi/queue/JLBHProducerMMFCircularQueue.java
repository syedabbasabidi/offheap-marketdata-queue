package com.abidi.queue;

import com.abidi.marketdata.model.MarketData;
import com.abidi.util.ByteUtils;
import com.abidi.util.ChecksumUtil;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.abidi.queue.CircularMMFQueue.getInstance;
import static com.abidi.queue.JLBHConsumerMMFCircularQueue.*;
import static java.lang.System.nanoTime;

public class JLBHProducerMMFCircularQueue implements JLBHTask {

    private JLBH jlbh;
    private CircularMMFQueue circularMMFQueue;
    private MarketData marketData;

    private Thread consumerThread;

    private static final Logger LOG = LoggerFactory.getLogger(JLBHProducerMMFCircularQueue.class);
    private int missedAdd;

    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS).iterations(ITERATIONS).throughput(THROUGHPUT).runs(RUNS).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHProducerMMFCircularQueue());


        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        marketData = new MarketData(new ByteUtils(), new ChecksumUtil());
        marketData.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", 1);
        try {
            circularMMFQueue = getInstance(marketData.size(), "/tmp");
            circularMMFQueue.reset();
            //consumerThread = new Thread(new CircularQueueConsumer(new ByteUtils()));
            //consumerThread.start();
        } catch (IOException e) {
            LOG.error("Error initializing test", e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        if (!circularMMFQueue.add(marketData.getData())) {
            missedAdd++;
        }
        jlbh.sampleNanos((nanoTime() - 10) - startTimeNS);
    }

    @Override
    public void complete() {
        LOG.info("Number of messages writen {} and read {}, missed writes {}", circularMMFQueue.messagesWritten(), circularMMFQueue.messagesRead(), missedAdd);
    }

}