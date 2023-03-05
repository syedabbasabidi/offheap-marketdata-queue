package model;

import util.ByteArrayUtil;
import util.ByteUtils;

import static util.ByteArrayUtil.byteToChar;
import static util.ByteUtils.bytesToLong;
import static util.ByteUtils.bytesToSecurity;

public class MarketDataCons {

    private static final int OBJ_SIZE = 45;
    private volatile byte[] data;
    private boolean isFirm;
    private int side;
    private char sec[] = new char[12];
    private double price;
    private char broker[] = new char[3];
    private int priceType;
    private char[] validUntil = new char[19];

    public MarketDataCons() {
        data = new byte[OBJ_SIZE];
    }

    public int getSide() {
        return side;
    }

    public char[] getSec() {
        return sec;
    }

    public double getPrice() {
        return price;
    }

    public char[] getBroker() {
        return broker;
    }

    public int getPriceType() {
        return priceType;
    }

    public char[] getValidUntil() {
        return validUntil;
    }

    public void firm() {
        isFirm = data[0] == 1;
    }

    public void side() {
        side = data[1];
    }

    public void security() {
        byteToChar(bytesToSecurity(data, 2, 12), this.sec);
    }

    public void price() {
        price = (double) bytesToLong(data, 14, 8) / 1000;
    }

    public void validUntil() {
        byteToChar(ByteUtils.bytesToDate(data, 22, 19), this.validUntil);
    }

    public void broker() {
        byteToChar(ByteUtils.bytesToBroker(data, 41, 3), this.broker);
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
        return String.valueOf(getSec()) + "-" + getPrice() + "-" + getSide() + "-" + (isFirm() ? "Firm" : "LL") + "-" + String.valueOf(getBroker()) + "-" + (getPriceType() == 1 ? "PoP" : "Unknown") + "-" + String.valueOf(getValidUntil());
    }

    public int size() {
        return OBJ_SIZE;
    }
}