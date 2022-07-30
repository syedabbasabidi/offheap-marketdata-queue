package model;

import util.ByteUtils;

import java.util.Arrays;

public class MarketData {

    private volatile byte[] data;

    public MarketData() {
        data = new byte[28];
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getSecurity() {
        return ByteUtils.bytesToCharSeq(Arrays.copyOfRange(data, 2, 18));
    }

    public double getPrice() {
        return (double) ByteUtils.bytesToLong(Arrays.copyOfRange(data, 19, 27)) / 1000;
    }

    public int side() {
       return Arrays.copyOfRange(data, 1, 2)[0];
    }

    public boolean isFirm() {
        return Arrays.copyOfRange(data, 0, 1)[0] == 1;
    }

    public void setSecurity(String id) {
        byte[] bytes = id.getBytes();
        for (int i =0,j=2; i < bytes.length; i++,j++) {
            data[j] = bytes[i];
        }
    }

    public void set(String secId, double price, int side, boolean isFirm) {
        this.setSecurity(secId);
        this.setPrice(price);
        this.side(side);
        this.setFirm(true);
    }

    public void setPrice(double price) {
        byte[] bytes = ByteUtils.longToBytes((long) (price * 1000));
        for (int i =0,j=19; i < bytes.length; i++,j++) {
            data[j] = bytes[i];
        }
    }

    public void side(int buy) {
        data[1] = (byte) buy;
    }

    public void setFirm(boolean isFirm) {
        data[0] = (byte) (isFirm ? 1 : 0);
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return "MarketData{" + "data= " +getSecurity() +"-" + getPrice() +"-"+ side() +"-"+ isFirm() +"}";
    }
}
