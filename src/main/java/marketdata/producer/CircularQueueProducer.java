package marketdata.producer;

import model.MarketData;
import queue.CircularMMFQueue;

import java.io.IOException;

import static queue.CircularMMFQueue.getInstance;

public class CircularQueueProducer {

    public static void main(String[] args) throws IOException, InterruptedException {

        MarketData marketData = new MarketData();
        CircularMMFQueue mmfQueue = getInstance(marketData.size());

        int j = 0;
        marketData.set("GB00BJLR0J16", 101.12d + j, 0, true, (byte) 1, "BRC", "2022-09-14:22:10:13");
        while (j < CircularMMFQueue.QUEUE_SIZE) {
            marketData.setPrice(101.12d + j);
            marketData.side(j % 2 == 0 ? 0 : 1);
            marketData.setFirm(j % 2 == 0);
            mmfQueue.add(marketData.getData());
            j++;
         /*   if (j % 10_000 == 0) {
                Thread.sleep(10);
            //    System.out.println(mmfQueue.getQueueSize());
            }*/
        }
    }

    private static String toMB(long init) {
        return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
    }
}