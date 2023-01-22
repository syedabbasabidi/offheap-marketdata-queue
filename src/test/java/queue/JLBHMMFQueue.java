package queue;

import model.MarketData;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class JLBHMMFQueue implements JLBHTask {

    private JLBH jlbh;
    private MMFQueue mmfQueue;
    private MarketData marketData;

    public List<Optional<byte[]>> ticks = new ArrayList(20_000_000);

    public static void main(String[] args) {

        JLBHOptions jlbhOptions = new JLBHOptions().throughput(5_000_000).iterations(4_000_000 - 20_000).runs(5).warmUpIterations(10_000).recordOSJitter(false).accountForCoordinatedOmission(true).jlbhTask(new JLBHMMFQueue());
        new JLBH(jlbhOptions).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        try {
            marketData = new MarketData();
            marketData.set("GB00BJLR0J16", 101.12d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13");
            mmfQueue = MMFQueue.getInstance(marketData.size());
            //     mmfQueue.add(marketData.getData());
        } catch (IOException e) {
            System.out.println("Queue initialization failed" + e);
        }
    }

    @Override
    public void run(long startTimeNS) {
        consumer(startTimeNS);
        //   producer(startTimeNS);
    }

    private void producer(long startTimeNS) {
        mmfQueue.add(marketData.getData());
        jlbh.sampleNanos((System.nanoTime() - 25) - startTimeNS);
    }

    private void consumer(long startTimeNS) {
        Optional<byte[]> optional = mmfQueue.get();
        jlbh.sampleNanos((System.nanoTime() - 25) - startTimeNS);
        ticks.add(optional);
    }

    public void complete() {
        JLBHTask.super.complete();
        System.out.println(ticks.get(100));
    }
}