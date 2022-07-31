package model;

import util.ByteUtils;

import java.util.Arrays;

import static java.util.Arrays.copyOfRange;
import static util.ByteUtils.bytesToLong;
import static util.ByteUtils.longToBytes;

public class MarketData {

    private static final int OBJ_SIZE = 28;
    private volatile byte[] data;

    public MarketData() {
        data = new byte[OBJ_SIZE];
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getSecurity() {
        return ByteUtils.bytesToCharSeq(copyOfRange(data, 2, 18));
    }

    public double getPrice() {
        return (double) bytesToLong(copyOfRange(data, 19, 27)) / 1000;
    }

    public int side() {
       return copyOfRange(data, 1, 2)[0];
    }

    public boolean isFirm() {
        return copyOfRange(data, 0, 1)[0] == 1;
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
        byte[] bytes = longToBytes((long) (price * 1000));
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


    public int size() {
        return OBJ_SIZE;
    }

}
