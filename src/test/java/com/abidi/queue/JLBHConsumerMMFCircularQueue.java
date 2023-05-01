package com.abidi.queue;

import com.abidi.marketdata.model.MarketData;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;

import java.io.IOException;

import static java.lang.System.nanoTime;
import static com.abidi.queue.CircularMMFQueue.getInstance;

public class JLBHConsumerMMFCircularQueue implements JLBHTask {

    private JLBH jlbh;
    private CircularMMFQueue circularMMFQueue;
    private MarketData md;
    private double price = 0;

    private Thread producerThread;
    private byte[] bytes;
    private int id;

    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(10_000).iterations(5_000_000).throughput(1_000_000).runs(3).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHConsumerMMFCircularQueue());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        md = new MarketData();
        md.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", id);
        try {
            circularMMFQueue = getInstance(md.size(), "/tmp");
            producerThread = new Thread(() -> {
                while (true) {
                    md.setPrice(++price);
                    md.setId(id++);
                    circularMMFQueue.add(md.getData());
                }
            });
            producerThread.start();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        bytes = circularMMFQueue.get();
        jlbh.sampleNanos((nanoTime() - 20) - startTimeNS);
    }

    @Override
    public void complete() {
        System.out.println(circularMMFQueue.messagesWritten() + "     " + circularMMFQueue.messagesRead());
    }

}