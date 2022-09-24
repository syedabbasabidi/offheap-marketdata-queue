package model;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

public class MarketDataPool {

    private final List<MarketData> marketDataPool;
    private final List<MarketData> inusePool;

    public MarketDataPool(int size) {
        marketDataPool = new LinkedList<>();
        IntStream.range(0, size).forEach(value -> marketDataPool.add(new MarketData()));
        this.inusePool = marketDataPool;
    }

    public Optional<MarketData> get(String secId, double price, int buy, boolean firm) {
        Optional<MarketData> marketData = get(null);
        marketData.ifPresent(data -> data.set(secId, price, buy, firm, (byte)1, "BRC", "2022-09-14:22:10:13"));
        return marketData;
    }

    public Optional<MarketData> getNew(String secId, double price, int buy, boolean firm) {
        MarketData marketData = new MarketData();
        marketData.set(secId, price, buy, firm, (byte)1, "BRC", "2022-09-14:22:10:13");
        return Optional.of(marketData);
    }

    public Optional<MarketData> get(byte[] data) {

        if (marketDataPool.size() > 0) {
            MarketData marketData = marketDataPool.remove(0);
            marketData.setData(data != null ? data : marketData.getData());
            inusePool.add(marketData);
            return Optional.of(marketData);
        }

        return Optional.empty();
    }

    public Optional<MarketData> getNew(byte[] data) {

        MarketData marketData = new MarketData();
        marketData.setData(data != null ? data : marketData.getData());
        return Optional.of(marketData);
    }

    public void ret(MarketData marketData) {

        marketDataPool.add(marketData);
        inusePool.remove(0);
    }

    public int objSize() {
        return 28;
    }

}
