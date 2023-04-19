package marketdata.consumer;

import model.MarketDataCons;
import queue.CircularMMFQueue;

import java.io.IOException;
import java.util.Optional;

import static java.lang.Integer.parseInt;
import static java.lang.Thread.interrupted;
import static queue.CircularMMFQueue.DEFAULT_SIZE;

public class CircularQueueConsumer implements Runnable {

    private final int howManyToConsume;

    public CircularQueueConsumer(int howManyToConsumer) {
        this.howManyToConsume = howManyToConsumer == -1 ? parseInt(System.getProperty("concount", "-1")) : -1;
    }

    public static void main(String[] args) {
        CircularQueueConsumer circularQueueConsumer = new CircularQueueConsumer(args.length > 0 ? parseInt(args[0]) : -1);
        circularQueueConsumer.run();
    }

    private boolean hasConsumedEnough(int totalConsumedMessages) {
        if (howManyToConsume == -1) return false;
        return howManyToConsume < totalConsumedMessages;
    }

    public void run() {

        System.out.println("Consumer starting...");
        MarketDataCons marketData = new MarketDataCons();
        CircularMMFQueue mmfQueue = getInstance(marketData);
        System.out.println("Reading to consume");
        int totalConsumedMessages = 0;
        while (true) {

            if (interrupted() || hasConsumedEnough(totalConsumedMessages)) break;

            Optional<byte[]> bytesOP = mmfQueue.get();
            bytesOP.ifPresent(bytes -> process(marketData, bytes));
            totalConsumedMessages++;
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