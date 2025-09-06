package com.abidi.marketdata.model;

import com.abidi.util.ByteArrayUtil;
import com.abidi.util.ByteUtils;

import static java.lang.String.valueOf;

public class MarketDataCons {

    private static final int OBJ_SIZE = 64;
    private final ByteUtils byteUtils;
    private byte[] data;
    private boolean isFirm;
    private int side;
    private final char[] sec = new char[12];
    private double price;
    private final char[] broker = new char[3];
    private int priceType;
    private final char[] validUntil = new char[19];
    private long checksum;

    private int id;

    public MarketDataCons(ByteUtils byteUtils) {
        this.byteUtils = byteUtils;
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

    public int getId() {
        return id;
    }

    public void firm() {
        isFirm = data[0] == 1;
    }

    public void side() {
        side = data[1];
    }

    public void security() {
        ByteArrayUtil.byteToChar(byteUtils.bytesToSecurity(data, 2, 12), this.sec);
    }

    public void price() {
        price = (double) byteUtils.bytesToLong(data, 14, 8) / 1000;
    }

    public void validUntil() {
        ByteArrayUtil.byteToChar(byteUtils.bytesToDate(data, 22, 19), this.validUntil);
    }

    public void broker() {
        ByteArrayUtil.byteToChar(byteUtils.bytesToBroker(data, 41, 3), this.broker);
    }

    public void priceType() {
        priceType = data[44];
    }

    public void id() {
        id = byteUtils.bytesToInt(data, 45, 4);
    }

    public void checksum() {
        checksum = byteUtils.bytesToLong(data, 49, 8);
    }


    public long getChecksum() {
        return checksum;
    }

    public void setData(byte[] data) {
        this.data = data;
        firm();
        side();
        security();
        price();
        validUntil();
        broker();
        priceType();
        id();
        checksum();

    }

    public boolean isFirm() {
        return isFirm;
    }

    @Override
    public String toString() {
        return getId() + "-" + valueOf(getSec()) + "-" + getPrice() + "-" + getSide() + "-" + (isFirm() ? "Firm" : "LL") + "-" + valueOf(getBroker()) + "-" + (getPriceType() == 1 ? "PoP" : "Unknown") + "-" + valueOf(getValidUntil());
    }

    public int size() {
        return OBJ_SIZE;
    }
}