package queue;

import model.MarketData;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;

import java.io.IOException;

public class JLBHMMFQueue implements JLBHTask {

    private JLBH jlbh;
    private MMFQueue mmfQueue;
    private MarketData marketData;

    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions().throughput(10_000_000).iterations(10_000_000-1_000).runs(10).warmUpIterations(10_000)
                                                   .recordOSJitter(true).jlbhTask(new JLBHMMFQueue());
        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        try {
            marketData = new MarketData();
            marketData.set("US2012DN31313HK11", 101.12d, 1, true);
            mmfQueue = MMFQueue.getInstance(marketData.size());
        } catch (IOException e) {
            System.out.println("Queue initialization failed" + e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        long start = System.nanoTime();
        mmfQueue.add(marketData.getData());
        jlbh.sampleNanos(System.nanoTime() - start);
    }
}