package marketdata.consumer;

import model.MarketDataCons;
import queue.CircularMMFQueue;

import java.io.IOException;
import java.util.Optional;

import static queue.CircularMMFQueue.DEFAULT_SIZE;

public class CircularQueueConsumer {

    public static void main(String[] args) {
        start();
    }

    public static void start() {

        System.out.println("Consumer starting...");
        MarketDataCons marketData = new MarketDataCons();
        CircularMMFQueue mmfQueue = getInstance(marketData);
        System.out.println("Reading to consume");
        while (true) {
            Optional<byte[]> bytesOP = mmfQueue.get();
            bytesOP.ifPresent(bytes -> process(marketData, bytes));
        }
    }

    private static void process(MarketDataCons marketData, byte[] data) {
        marketData.setData(data);
        System.out.println(marketData);
    }

    private static CircularMMFQueue getInstance(MarketDataCons marketData) {
        try {
            return CircularMMFQueue.getInstance(marketData.size(), DEFAULT_SIZE, "/tmp");
        } catch (IOException e) {
            System.out.println(e);
            return null;
        }
    }
}