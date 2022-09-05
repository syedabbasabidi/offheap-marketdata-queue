package marketdata.consumer;

import model.MarketData;
import queue.MMFQueue;

import java.io.IOException;

import static java.lang.System.nanoTime;
import static java.util.stream.IntStream.range;
import static queue.MMFQueue.QUEUE_SIZE;
import static queue.MMFQueue.getInstance;

public class QueueConsumer {
    private static MarketData arr[] = new MarketData[1000];

    public static void main(String[] args) throws IOException {

        MarketData marketData = new MarketData();
        MMFQueue mmfQueue = getInstance(marketData.size());
        long startTime = nanoTime();
        range(0, QUEUE_SIZE).forEach(i -> mmfQueue.get().ifPresent(data -> process(marketData, i, data)));
        System.out.println((nanoTime() - startTime) / 1000_000 + " ms");
        System.out.println(arr[455]);
        System.out.println("Messages Written: "+mmfQueue.messagesWritten()+", Messages Read: "+mmfQueue.messagesRead());

    }

    private static void process(MarketData marketData, int i, byte[] data) {
        marketData.setData(data);
        if (i % 100_000 == 0) arr[i / 100_000] = marketData;
      //  System.out.println(marketData);
    }
}