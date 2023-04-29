package com.abidi.queue;

import com.abidi.marketdata.consumer.CircularQueueConsumer;
import com.abidi.model.MarketData;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.System.nanoTime;
import static com.abidi.queue.CircularMMFQueue.getInstance;

public class JLBHMMFCircularQueue implements JLBHTask {

    private JLBH jlbh;
    private CircularMMFQueue circularMMFQueue;
    private MarketData marketData;
    private double price = 0;

    private Thread consumerThread;

    private static final Logger LOG = LoggerFactory.getLogger(JLBHMMFCircularQueue.class);

    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(10_000).iterations(5_000_000).throughput(1_000_000).runs(3).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHMMFCircularQueue());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        marketData = new MarketData();
        marketData.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", 1);
        try {
            circularMMFQueue = getInstance(marketData.size(), "/tmp");
            circularMMFQueue.reset();
            consumerThread = new Thread(new CircularQueueConsumer(-1));
            consumerThread.start();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        marketData.setPrice(++price);
        circularMMFQueue.add(marketData.getData());
        jlbh.sampleNanos((nanoTime() - 20) - startTimeNS);
    }

    @Override
    public void complete() {
        consumerThread.interrupt();
        LOG.info("Number of messages wrriten {} and read {}", circularMMFQueue.messagesWritten(), circularMMFQueue.messagesRead());
    }

}