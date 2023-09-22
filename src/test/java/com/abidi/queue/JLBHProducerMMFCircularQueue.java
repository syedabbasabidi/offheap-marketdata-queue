package com.abidi.queue;

import com.abidi.consumer.CircularQueueConsumer;
import com.abidi.marketdata.model.MarketData;
import com.abidi.util.ByteUtils;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.abidi.queue.CircularMMFQueue.getInstance;
import static java.lang.System.nanoTime;

public class JLBHProducerMMFCircularQueue implements JLBHTask {

    private JLBH jlbh;
    private CircularMMFQueue circularMMFQueue;
    private MarketData marketData;
    private double price = 0;

    private Thread consumerThread;

    private static final Logger LOG = LoggerFactory.getLogger(JLBHProducerMMFCircularQueue.class);

    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(10_000).iterations(5_000_000).throughput(1_000_000).runs(3).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHProducerMMFCircularQueue());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        marketData = new MarketData(new ByteUtils());
        marketData.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", 1);
        try {
            circularMMFQueue = getInstance(marketData.size(), "/tmp");
            circularMMFQueue.reset();
            consumerThread = new Thread(new CircularQueueConsumer());
            consumerThread.start();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        circularMMFQueue.add(marketData.getData());
        jlbh.sampleNanos((nanoTime() - 10) - startTimeNS);
    }

    @Override
    public void complete() {
        consumerThread.interrupt();
        LOG.info("Number of messages writen {} and read {}", circularMMFQueue.messagesWritten(), circularMMFQueue.messagesRead());
    }

}