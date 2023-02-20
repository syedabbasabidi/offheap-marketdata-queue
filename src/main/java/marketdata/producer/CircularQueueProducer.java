package marketdata.producer;

import model.MarketData;
import queue.CircularMMFQueue;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.util.List;

public class CircularQueueProducer {

    public static void main(String[] args) throws IOException, InterruptedException {

        MarketData marketData = new MarketData();
        CircularMMFQueue mmfQueue = CircularMMFQueue.getInstance(marketData.size());

        new Thread(() -> {
            try {
                logMem();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        int j = 0;
        marketData. set("GB00BJLR0J16", 101.12d + j, 0, true, (byte) 1, "BRC", "2022-09-14:22:10:13");
        while (true) {
            marketData.setPrice(101.12d + j);
            marketData.side(j % 2 == 0 ? 0 : 1);
            marketData.setFirm(j % 2 == 0);
            mmfQueue.add(marketData.getData());
            j++;
            if (j % 5000 == 0) {
                Thread.sleep(10);
            }
        }
    }

    private static void logMem() throws InterruptedException {

        while (true) {
            List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            for (BufferPoolMXBean pool : pools) {
                System.out.println(pool.getName());
                System.out.println(pool.getCount());
                System.out.println(LocalDateTime.now() +"memory used " + toMB(pool.getMemoryUsed()));
                System.out.println("total capacity" + toMB(pool.getTotalCapacity()));
            }
            System.out.println("heap mem used: "+toMB(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
            System.out.println("heap mem total: "+toMB(Runtime.getRuntime().maxMemory()));
            System.out.println();
            Thread.sleep(1000);
        }
    }

    private static String toMB(long init) {
        return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
    }
}