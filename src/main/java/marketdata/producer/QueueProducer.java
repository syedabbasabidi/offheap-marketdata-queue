package marketdata.producer;

import model.MarketData;
import queue.MMFQueue;

import java.io.IOException;
import java.util.stream.IntStream;

import static java.lang.System.nanoTime;
import static queue.MMFQueue.QUEUE_SIZE;

public class QueueProducer {

    public static void main(String[] args) throws IOException {

        MarketData marketData = new MarketData();
        MMFQueue mmfQueue = MMFQueue.getInstance(marketData.size());

        long startTime = nanoTime();
        IntStream.range(0, QUEUE_SIZE).forEach(j -> {
            marketData.set("GB00BJLR0J16", 101.12d + j, j % 2 == 0 ? 0 : 1, j % 2 == 0, (byte)1, "BRC", "2022-09-14:22:10:13");
            mmfQueue.add(marketData.getData());

        });
        System.out.println((nanoTime() - startTime)/1000_000+" ms");
        System.out.println("Messages Written: "+mmfQueue.messagesWritten()+", Messages Read: "+mmfQueue.messagesRead());
    }
}