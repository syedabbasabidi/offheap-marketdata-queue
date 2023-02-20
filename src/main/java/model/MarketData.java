package model;

import util.ByteUtils;

import java.nio.ByteBuffer;

import static java.util.Arrays.copyOfRange;
import static util.ByteUtils.bytesToLong;
import static util.ByteUtils.longToBytes;

public class MarketData {

    private static final int OBJ_SIZE = 45;
    private volatile byte[] data;

    private final ByteBuffer securityMapper = ByteBuffer.allocateDirect(12);
    private final ByteBuffer brokerMapper = ByteBuffer.allocateDirect(3);
    private final ByteBuffer dateMapper = ByteBuffer.allocateDirect(19);

    public MarketData() {
        data = new byte[OBJ_SIZE];
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setFirm(boolean isFirm) {
        data[0] = (byte) (isFirm ? 1 : 0);
    }

    public void side(int buy) {
        data[1] = (byte) buy;
    }

    public void setSecurity(String id) {
        for(int i=0; i<id.length(); i++) {
            securityMapper.put((byte) id.charAt(i));
        }
        for (int i = 0; i < 12; i++) {
            data[i] = securityMapper.get();
        }
    }

    public void setPrice(double price) {
        byte[] bytes = longToBytes((long) (price * 1000));
        for (int i = 0, j = 14; i < bytes.length; i++, j++) {
            data[j] = bytes[i];
        }
    }

    public void setExpiresAt(String expiresAt) {

        for(int i=0; i<expiresAt.length(); i++) {
            dateMapper.put((byte) expiresAt.charAt(i));
        }

        for (int i = 0, j = 22; i < 19; i++, j++) {
            data[j] = dateMapper.get();
        }
    }

    public void setBroker(String broker) {

        for(int i=0; i<broker.length(); i++) {
            brokerMapper.put((byte) broker.charAt(i));
        }

        for (int i = 0, j = 41; i < 3; i++, j++) {
            data[j] = brokerMapper.get();
        }
    }

    public void setPriceType(byte priceType) {
        data[44] = priceType;
    }


    public boolean isFirm() {
        return copyOfRange(data, 0, 1)[0] == 1;
    }

    public int side() {
        return copyOfRange(data, 1, 2)[0];
    }

    public String getSecurity() {
        return ByteUtils.bytesToSecurity(copyOfRange(data, 2, 14));
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


    public void set(String secId, double price, int side, boolean isFirm, byte priceType, String broker, String expiresAt) {
        this.setSecurity(secId);
        this.setPrice(price);
        this.side(side);
        this.setFirm(isFirm);
        this.setPriceType(priceType);
        this.setBroker(broker);
        this.setExpiresAt(expiresAt);

    }


    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return getSecurity() + "-" + getPrice() + "-" + side() + "-" + (isFirm() ? "Firm" : "LL") + "-" + getBroker() + "-" + (getPriceType() == 1 ? "Price" : "Unknown") + "-" + getValidUntil();
    }

    public int size() {
        return OBJ_SIZE;
    }
}