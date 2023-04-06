package queue;

import marketdata.consumer.CircularQueueConsumer;
import model.MarketData;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;

import java.io.IOException;

import static queue.CircularMMFQueue.getInstance;

public class JLBHMMFCircularQueue implements JLBHTask {

    private JLBH jlbh;
    private CircularMMFQueue circularMMFQueue;
    private MarketData marketData;
    private double price = 0;

    private Thread consumerThread;

    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions().warmUpIterations(5).iterations(10).throughput(1_000_000)
                .accountForCoordinatedOmission(false).recordOSJitter(false)
                .jlbhTask(new JLBHMMFCircularQueue());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        marketData = new MarketData();
        marketData.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13");
        try {
            consumerThread = new Thread(CircularQueueConsumer::start);
            consumerThread.start();
            circularMMFQueue = getInstance(marketData.size(), CircularMMFQueue.DEFAULT_SIZE, "/tmp");
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        marketData.setPrice(++price);
        circularMMFQueue.add(marketData.getData());
        jlbh.sampleNanos(System.nanoTime() - startTimeNS);
    }

    @Override
    public void complete() {
        consumerThread.interrupt();
    }

}