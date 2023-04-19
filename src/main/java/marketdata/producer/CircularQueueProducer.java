package marketdata.producer;

import model.MarketData;
import queue.CircularMMFQueue;

import java.io.IOException;

import static queue.CircularMMFQueue.DEFAULT_SIZE;
import static queue.CircularMMFQueue.getInstance;

public class CircularQueueProducer {

    public static final int BATCH_SIZE = 10;

    public static void main(String[] args) throws IOException {

        MarketData md = new MarketData();
        CircularMMFQueue mmfQueue = getInstance(md.size(), "/tmp");

        int j = 0;
        md.set("GB00BJLR0J16", 1d + j, 0, true, (byte) 1, "BRC", "2023-02-14:22:10:13");
        while (true) {
            md.setPrice(1d + j);
            md.side(j % 2 == 0 ? 0 : 1);
            md.setFirm(j % 2 == 0);
            j = mmfQueue.add(md.getData()) ? j + 1 : j;
            if (j > 0 && j % BATCH_SIZE == 0) pause(mmfQueue, (j / BATCH_SIZE));

        }
    }

    private static void pause(CircularMMFQueue mmfQueue, int batchNumber) {
        try {
            System.out.println("Wrote batch number " + batchNumber + ", size " + mmfQueue.getQueueSize());
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static String toMB(long init) {
        return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
    }
}