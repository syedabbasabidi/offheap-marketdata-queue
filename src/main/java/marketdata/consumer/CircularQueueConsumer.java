package marketdata.consumer;

import model.MarketDataCons;
import queue.CircularMMFQueue;

import java.io.IOException;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.lang.Thread.interrupted;

public class CircularQueueConsumer implements Runnable {

    private final int howManyToConsume;

    public CircularQueueConsumer(int howManyToConsumer) {
        this.howManyToConsume = howManyToConsumer == -1 ? parseInt(getProperty("concount", "-1")) : -1;
    }

    public static void main(String[] args) {
        CircularQueueConsumer circularQueueConsumer = new CircularQueueConsumer(args.length > 0 ? parseInt(args[0]) : -1);
        circularQueueConsumer.run();
    }

    private boolean hasConsumedEnough(int totalConsumedMessages) {
        if (howManyToConsume == -1) return false;
        return howManyToConsume <= totalConsumedMessages;
    }

    public void run() {

        System.out.println("Consumer starting...");
        MarketDataCons marketData = new MarketDataCons();
        CircularMMFQueue mmfQueue = getInstance(marketData);
        System.out.println("Reading to consume");
        int totalConsumedMessages = 0;
        while (true) {

            if (interrupted() || hasConsumedEnough(totalConsumedMessages)) break;

            byte[] bytes = mmfQueue.get();
            if (bytes != null) process(marketData, bytes);
            totalConsumedMessages++;
        }
    }


    private static void process(MarketDataCons marketData, byte[] data) {
        marketData.setData(data);
        System.out.println(marketData);
    }

    private static CircularMMFQueue getInstance(MarketDataCons marketData) {
        try {
            return CircularMMFQueue.getInstance(marketData.size(), "/tmp");
        } catch (IOException e) {
            System.out.println(e);
            return null;
        }
    }
}