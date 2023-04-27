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

        JLBHOptions jlbhOptions = new JLBHOptions()
                .warmUpIterations(10_000).iterations(1_000_000).throughput(1_000_000).accountForCoordinatedOmission(false)
                .recordOSJitter(false).jlbhTask(new JLBHMMFCircularQueue());

        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {

        this.jlbh = jlbh;
        marketData = new MarketData();
        marketData.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13", 1);
        try {
            consumerThread = new Thread(new CircularQueueConsumer(-1));
            consumerThread.start();
            circularMMFQueue = getInstance(marketData.size(), "/tmp");
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        marketData.setPrice(++price);
        circularMMFQueue.add(marketData.getData());
        jlbh.sampleNanos((System.nanoTime() - 20) - startTimeNS);
    }

    @Override
    public void complete() {
        //consumerThread.interrupt();
        System.out.println(circularMMFQueue.messagesWritten() + "     " + circularMMFQueue.messagesRead());
    }

}