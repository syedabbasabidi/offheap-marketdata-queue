package marketdata.consumer;

import model.MarketDataCons;
import queue.CircularMMFQueue;

import java.io.IOException;
import java.util.Optional;

public class CircularQueueConsumer {
    private static MarketDataCons arr[] = new MarketDataCons[1000];

    public static void main(String[] args) throws IOException {
        start(false);
    }

    public static void start(boolean log) {

        System.out.println("Consumer starting...");
        MarketDataCons marketData = new MarketDataCons();
        CircularMMFQueue mmfQueue = getInstance(marketData);
        int i = 0;
        System.out.println("Reading to consume");
        while (true) {
            if (Thread.interrupted()) {
                System.out.println("Stopping Consumer");
                break;
            }
            Optional<byte[]> bytesOP = mmfQueue.get();
            if (bytesOP.isPresent()) process(marketData, i++, bytesOP.get());
        }
    }

    private static void process(MarketDataCons marketData, int i, byte[] data) {
        marketData.setData(data);
        if (i % 100_000 == 0) arr[i / 100_000] = marketData;
        System.out.println(marketData);
    }

    public static MarketDataCons[] getCapturedMD() {
        return arr;
    }

    private static CircularMMFQueue getInstance(MarketDataCons marketData) {
        try {
            return CircularMMFQueue.getInstance(marketData.size());
        } catch (IOException e) {
            System.out.println(e);
            return null;
        }
    }
}