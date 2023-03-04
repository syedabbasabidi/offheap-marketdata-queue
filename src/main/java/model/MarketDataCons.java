package model;

import util.ByteUtils;

import static java.util.Arrays.copyOfRange;
import static util.ByteUtils.bytesToLong;
import static util.ByteUtils.bytesToSecurity;

public class MarketDataCons {

    private static final int OBJ_SIZE = 45;
    private volatile byte[] data;
    private boolean isFirm;
    private int side;
    private StringBuilder sec;
    private double price;
    private StringBuilder broker;
    private int priceType;
    private StringBuilder validUntil;

    public MarketDataCons() {
        data = new byte[OBJ_SIZE];
    }



    public boolean isFirm() {
        return copyOfRange(data, 0, 1)[0] == 1;
    }

    public int side() {
        return copyOfRange(data, 1, 2)[0];
    }

    public String getSecurity() {
        return bytesToSecurity(copyOfRange(data, 2, 14));
    }

    public double getPrice() {
        return (double) bytesToLong(copyOfRange(data, 14, 22)) / 1000;
    }

    public String getValidUntil() {
        return ByteUtils.bytesToDate(copyOfRange(data, 22, 41));
    }

    public String getBroker() {
        return ByteUtils.bytesToBroker(copyOfRange(data, 41, 44));
    }

    public int getPriceType() {
        return copyOfRange(data, 44, 45)[0];
    }



    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return getSecurity() + "-" + getPrice() + "-" + side() + "-" + (isFirm() ? "Firm" : "LL") + "-" + getBroker() + "-" + (getPriceType() == 1 ? "PoP" : "Unknown") + "-" + getValidUntil();
    }

    public int size() {
        return OBJ_SIZE;
    }
}