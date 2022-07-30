package marketdata.producer;

import model.MarketData;
import model.MarketDataPool;
import queue.MMFQueue;

import java.io.IOException;
import java.util.Optional;

public class QueueProducer {

    public static void main(String[] args) throws IOException, InterruptedException {


        MarketDataPool marketDataPool = new MarketDataPool(10_000);
        MMFQueue mmfQueue = null;
        mmfQueue = new MMFQueue(marketDataPool.objSize(), 100_000_000, "FAST_QUEUE", true);

        long startTime = System.nanoTime();
        for (int j = 0; j < 100_000_000; j++) {
            Optional<MarketData> marketData = marketDataPool.get("US2012DN", 101.12d + j, j % 2 == 0 ? 0 : 1, j % 2 == 0);
            mmfQueue.add(marketData.get().getData());
            marketDataPool.ret(marketData.get());
        }

        System.out.println(System.nanoTime() - startTime);
        System.out.println(mmfQueue.getSize());
    }
}