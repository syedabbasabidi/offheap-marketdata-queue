package com.abidi.model;

import com.abidi.util.ByteArrayUtil;
import com.abidi.util.ByteUtils;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;
import static java.util.stream.IntStream.range;

public class MarketData {

    private static final int OBJ_SIZE = 49;
    private volatile byte[] data;

    private final ByteBuffer securityMapper = allocateDirect(12);
    private final ByteBuffer brokerMapper = allocateDirect(3);
    private final ByteBuffer dateMapper = allocateDirect(19);

    public MarketData() {
        data = new byte[OBJ_SIZE];
    }

    public void setData(byte[] data) {
        ByteArrayUtil.copy(data, this.data);
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

        byte[] bytes = ByteUtils.longToBytes((long) (price * 1000));

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

    public void setId(int id) {
        byte[] bytes = ByteUtils.intToBytes(id);
        for (int i = 0, j = 45; i < bytes.length; i++, j++) {
            data[j] = bytes[i];
        }
    }

    public void set(String secId, double price, int side, boolean isFirm, byte priceType, String broker, String expiresAt, int id) {
        this.setSecurity(secId);
        this.setPrice(price);
        this.side(side);
        this.setFirm(isFirm);
        this.setPriceType(priceType);
        this.setBroker(broker);
        this.setExpiresAt(expiresAt);
        this.setId(id);
    }


    public byte[] getData() {
        return data;
    }


    public int size() {
        return OBJ_SIZE;
    }
}