package marketdata.consumer;

import model.MarketData;
import queue.CircularMMFQueue;

import java.io.IOException;
import java.util.Optional;

public class CircularQueueConsumer {
    private static MarketData arr[] = new MarketData[1000];

    public static void main(String[] args) throws IOException {

        start(false);
        //System.out.println((nanoTime() - startTime) / 1000_000 + " ms");
        //System.out.println(arr[455]);
        //System.out.println("Messages Written: " + mmfQueue.messagesWritten() + ", Messages Read: " + mmfQueue.messagesRead());

    }

    public static void start(boolean log) {

        System.out.println("Consumer starting...");
        MarketData marketData = new MarketData();
        CircularMMFQueue mmfQueue = getInstance(marketData);
        int i = 0;
        System.out.println("Reading to consume");
        while (true) {
            if(Thread.interrupted()) {
                System.out.println("Stopping Consumer");
                break;
            }
            Optional<byte[]> bytesOP = mmfQueue.get();
            if (bytesOP.isPresent()) process(marketData, i++, bytesOP.get());
        }
    }

    private static CircularMMFQueue getInstance(MarketData marketData) {
        try {
            return CircularMMFQueue.getInstance(marketData.size());
        } catch (IOException e) {
            System.out.println(e);
            return null;
        }
    }

    private static void process(MarketData marketData, int i, byte[] data) {
        marketData.setData(data);
        if (i % 100_000 == 0) arr[i / 100_000] = marketData;
        System.out.println(marketData);
    }

    public static MarketData[] getCapturedMD() {
        return arr;
    }
}