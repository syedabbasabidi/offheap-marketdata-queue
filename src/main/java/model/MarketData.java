package model;

import util.ByteUtils;

import java.nio.ByteBuffer;

import static java.util.Arrays.copyOfRange;
import static java.util.stream.IntStream.range;
import static util.ByteUtils.*;

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
        range(0, id.length()).map(id::charAt).forEach(b -> securityMapper.put((byte) b));
        securityMapper.flip();
        range(2, 14).forEach(i -> data[i] = securityMapper.get());
    }

    public void setPrice(double price) {

        byte[] bytes = longToBytes((long) (price * 1000));

        for (int i = 0, j = 14; i < bytes.length; i++, j++) {
            data[j] = bytes[i];
        }
    }

    public void setExpiresAt(String expiresAt) {

        range(0, expiresAt.length()).map(expiresAt::charAt).forEach(b -> dateMapper.put((byte) b));
        dateMapper.flip();
        for (int i = 0, j = 22; i < 19; i++, j++) {
            data[j] = dateMapper.get();
        }
    }

    public void setBroker(String broker) {

        range(0, broker.length()).map(broker::charAt).forEach(b -> brokerMapper.put((byte) b));
        brokerMapper.flip();
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
        return getSecurity() + "-" + getPrice() + "-" + side() + "-" + (isFirm() ? "Firm" : "LL") + "-" + getBroker() + "-" + (getPriceType() == 1 ? "PoP" : "Unknown") + "-" + getValidUntil();
    }

    public int size() {
        return OBJ_SIZE;
    }
}