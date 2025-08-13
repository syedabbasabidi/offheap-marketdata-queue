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
import static java.lang.System.nanoTime;

public class JLBHConsumerMMFCircularQueue implements JLBHTask {

    public static final int ITERATIONS = 5_000_000;
    public static final int THROUGHPUT = 1_000_000;
    public static final int RUNS = 16;
    public static final int WARM_UP_ITERATIONS = 10_000;
    private JLBH jlbh;
    private CircularMMFQueue circularMMFQueue;
    private MarketData md;
    private double price = 0;

    private Thread producerThread;
    private byte[] bytes;
    private int id;

    private static final Logger LOG = LoggerFactory.getLogger(JLBHConsumerMMFCircularQueue.class);


    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(WARM_UP_ITERATIONS).iterations(ITERATIONS).throughput(THROUGHPUT).runs(RUNS).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHConsumerMMFCircularQueue());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        md = new MarketData(new ByteUtils(), new ChecksumUtil());
        md.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", id);
        try {
            circularMMFQueue = getInstance(md.size(), "/tmp");
            producerThread = new Thread(() -> {
                while (!producerThread.isInterrupted()) {
                    md.setPrice(++price);
                    md.setId(id++);
                    circularMMFQueue.add(md.getData());
                }
            });
            producerThread.setName("JLBH Producer");
            producerThread.start();
        } catch (IOException e) {
            LOG.error("Error initializing test", e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        bytes = circularMMFQueue.get();
        jlbh.sampleNanos((nanoTime() - 10) - startTimeNS);
    }

    @Override
    public void complete() {
        LOG.info("Number of messages writen {} and read {}", circularMMFQueue.messagesWritten(), circularMMFQueue.messagesRead());
        producerThread.interrupt();
    }

}