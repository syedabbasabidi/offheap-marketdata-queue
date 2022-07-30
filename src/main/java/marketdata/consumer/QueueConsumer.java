package marketdata.consumer;

import model.MarketData;
import model.MarketDataPool;
import queue.MMFQueue;

import java.io.IOException;
import java.util.Optional;

public class QueueConsumer {

    private static MarketData arr[] = new MarketData[1000];

    public static void main(String[] args) throws IOException {

        MarketDataPool marketDataPool = new MarketDataPool(10_000);
        MMFQueue mmfQueue = new MMFQueue(marketDataPool.objSize(), 100_000_000, "FAST_QUEUE", true);
        long startTime = System.nanoTime();
        for (int i = 0; i < 100_000_000; i++) {
            if (mmfQueue.get().isPresent()) {
                Optional<MarketData> a = marketDataPool.get(mmfQueue.get().get());
                if (a.isPresent()) {arr[i % 1000] = a.get();}
                marketDataPool.ret(a.get());
            }
        }
        System.out.println(System.nanoTime() - startTime);
        System.out.println(arr[234]);

    }
}