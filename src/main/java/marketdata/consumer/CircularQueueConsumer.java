package marketdata.consumer;

import model.MarketData;
import queue.CircularMMFQueue;

import java.io.IOException;

import static java.lang.System.nanoTime;

public class CircularQueueConsumer {
    private static MarketData arr[] = new MarketData[1000];

    public static void main(String[] args) throws IOException {

        MarketData marketData = new MarketData();
        CircularMMFQueue mmfQueue = CircularMMFQueue.getInstance(marketData.size());
        long startTime = nanoTime();
        int i = 0;
        while (true) {
            mmfQueue.get().ifPresent(data -> process(marketData, i, data));
        }
        //System.out.println((nanoTime() - startTime) / 1000_000 + " ms");
        //System.out.println(arr[455]);
        //System.out.println("Messages Written: " + mmfQueue.messagesWritten() + ", Messages Read: " + mmfQueue.messagesRead());

    }

    private static void process(MarketData marketData, int i, byte[] data) {
        marketData.setData(data);
        if (i % 100_000 == 0) arr[i / 100_000] = marketData;
        //  System.out.println(marketData);
    }
}