package marketdata.producer;

import model.MarketData;
import queue.CircularMMFQueue;

import java.io.IOException;

import static java.lang.System.nanoTime;

public class CircularQueueProducer {

    public static void main(String[] args) throws IOException {

        MarketData marketData = new MarketData();
        CircularMMFQueue mmfQueue = CircularMMFQueue.getInstance(marketData.size());

        long startTime = nanoTime();
        int j  = 0;
        while(true) {
            marketData.set("GB00BJLR0J16", 101.12d + j, j % 2 == 0 ? 0 : 1, j % 2 == 0, (byte)1, "BRC", "2022-09-14:22:10:13");
            mmfQueue.add(marketData.getData());
            j++;
        }
//        System.out.println((nanoTime() - startTime)/1000_000+" ms");
  //      System.out.println("Messages Written: "+mmfQueue.messagesWritten()+", Messages Read: "+mmfQueue.messagesRead());
    }
}