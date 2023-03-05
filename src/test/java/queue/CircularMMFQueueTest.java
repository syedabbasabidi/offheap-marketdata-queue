package queue;

import model.MarketData;
import org.junit.Test;

import java.io.IOException;

public class CircularMMFQueueTest {

    @Test
    public void testProducer() throws IOException {

        MarketData md = new MarketData();
        CircularMMFQueue queue = CircularMMFQueue.getInstance(md.size());
        md.set("GB00BJLR0J16", 0d, 1, true, (byte) 1, "BRC", "2022-09-14:22:10:13");
        for (int j = 1; j <= 10_000; j++) {
            //    md.setPrice(md.getPrice() + j);
            if (queue.add(md.getData())) {
                System.out.println(queue);
            }
        }

    }

}