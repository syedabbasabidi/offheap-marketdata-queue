package model;

import util.ByteArrayUtil;
import util.ByteUtils;

import static util.ByteUtils.bytesToLong;
import static util.ByteUtils.bytesToSecurity;

public class MarketDataCons {

    private static final int OBJ_SIZE = 45;
    private volatile byte[] data;
    private boolean isFirm;
    private int side;
    private String sec;
    private double price;
    private String broker;
    private int priceType;
    private String validUntil;

    public MarketDataCons() {
        data = new byte[OBJ_SIZE];
    }

    public int getSide() {
        return side;
    }

    public String getSec() {
        return sec;
    }

    public double getPrice() {
        return price;
    }

    public String getBroker() {
        return broker;
    }

    public int getPriceType() {
        return priceType;
    }

    public String getValidUntil() {
        return validUntil;
    }

    public void firm() {
        isFirm = data[0] == 1;
    }

    public void side() {
        side = data[1];
    }

    public void security() {
        sec = bytesToSecurity(data, 2, 12);
    }

    public void price() {
        price = (double) bytesToLong(data, 14, 8) / 1000;
    }

    public void validUntil() {
        validUntil = ByteUtils.bytesToDate(data, 22, 19);
    }

    public void broker() {
        broker = ByteUtils.bytesToBroker(data, 41, 3);
    }

    public void priceType() {
        priceType = data[44];
    }

    public void setData(byte[] data) {
        ByteArrayUtil.copy(data, this.data);
        firm();
        side();
        security();
        price();
        validUntil();
        broker();
        priceType();
    }

    public boolean isFirm() {
        return isFirm;
    }

    @Override
    public String toString() {
        return getSec() + "-" + getPrice() + "-" + getSide() + "-" + (isFirm() ? "Firm" : "LL") + "-" + getBroker() + "-" + (getPriceType() == 1 ? "PoP" : "Unknown") + "-" + getValidUntil();
    }

    public int size() {
        return OBJ_SIZE;
    }
}