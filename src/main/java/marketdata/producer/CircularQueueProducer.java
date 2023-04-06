package marketdata.producer;

import model.MarketData;
import queue.CircularMMFQueue;

import java.io.IOException;

import static queue.CircularMMFQueue.DEFAULT_SIZE;
import static queue.CircularMMFQueue.getInstance;

public class CircularQueueProducer {

    public static final int BATCH_SIZE = 1_000;

    public static void main(String[] args) throws IOException {

        MarketData md = new MarketData();
        CircularMMFQueue mmfQueue = getInstance(md.size(), DEFAULT_SIZE, "/tmp");

        int j = 0;
        md.set("GB00BJLR0J16", 1d + j, 0, true, (byte) 1, "BRC", "2023-02-14:22:10:13");
        while (true) {
            md.setPrice(1d + j);
            md.side(j % 2 == 0 ? 0 : 1);
            md.setFirm(j % 2 == 0);
            j = mmfQueue.add(md.getData()) ? j + 1 : j;
            if (j % BATCH_SIZE == 0) pause((j / BATCH_SIZE) + 1);

        }
    }

    private static void pause(int batchNumber) {
        try {
            System.out.println("Wrote batch number " + batchNumber);
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static String toMB(long init) {
        return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
    }
}