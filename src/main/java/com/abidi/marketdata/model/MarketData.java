package com.abidi.marketdata.model;

import com.abidi.util.ByteUtils;
import com.abidi.util.ChecksumUtil;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;

public class MarketData {

    private static final int OBJ_SIZE = 64;
    public static final int SECURITY_START_INDEX = 2;
    public static final int FIRM_QUOTE_INDICATOR_INDEX = 0;
    public static final int SIDE_INDICATOR_INDEX = 1;
    public static final int PRICE_START_INDEX = 14;
    public static final int QUOTE_EXPIRY_START_INDEX = 22;
    public static final int QUOTING_BROKER_START_INDEX = 41;
    public static final int PRICE_TYPE_START_INDEX = 44;
    public static final int QUOTE_ID_START_INDEX = 45;
    public static final int CHECKSUM_INDEX = 49;
    public static final int CHECKSUM_SIZE_IN_BYTES = 8;
    private final byte[] data;

    private final ByteBuffer securityMapper = allocateDirect(12);
    private final ByteBuffer brokerMapper = allocateDirect(3);
    private final ByteBuffer dateMapper = allocateDirect(19);

    private final ByteUtils byteUtils;
    private final ChecksumUtil checksumUtil;

    public MarketData(ByteUtils byteUtils, ChecksumUtil checksumUtil) {
        this.checksumUtil = checksumUtil;
        data = new byte[OBJ_SIZE];
        this.byteUtils = byteUtils;
    }

    public void setFirm(boolean isFirm) {
        data[FIRM_QUOTE_INDICATOR_INDEX] = (byte) (isFirm ? 1 : 0);
    }

    public void side(int buy) {
        data[SIDE_INDICATOR_INDEX] = (byte) buy;
    }

    public void setSecurity(String id) {
        //dump security in sec buffer
        for (int i1 = 0; i1 < id.length(); i1++) {
            int b = id.charAt(i1);
            securityMapper.put((byte) b);
        }
        //Make sec buffer readable
        securityMapper.flip();
        //dump security in object buffer
        for (int i = SECURITY_START_INDEX; i < PRICE_START_INDEX; i++) {
            data[i] = securityMapper.get();
        }
        securityMapper.clear();
    }

    public void setPrice(double price) {
        byte[] bytes = byteUtils.longToBytes((long) (price * 1000));
        for (int i = 0, j = PRICE_START_INDEX; i < bytes.length; i++, j++) {
            data[j] = bytes[i];
        }
    }

    public void setExpiresAt(String expiresAt) {

        for (int i1 = 0; i1 < expiresAt.length(); i1++) {
            int b = expiresAt.charAt(i1);
            dateMapper.put((byte) b);
        }
        dateMapper.flip();
        for (int i = 0, j = QUOTE_EXPIRY_START_INDEX; i < 19; i++, j++) {
            data[j] = dateMapper.get();
        }
        dateMapper.clear();
    }

    public void setBroker(String broker) {

        for (int i1 = 0; i1 < broker.length(); i1++) {
            int b = broker.charAt(i1);
            brokerMapper.put((byte) b);
        }
        brokerMapper.flip();
        for (int i = 0, j = QUOTING_BROKER_START_INDEX; i < 3; i++, j++) {
            data[j] = brokerMapper.get();
        }
        brokerMapper.clear();
    }

    public void setPriceType(byte priceType) {
        data[PRICE_TYPE_START_INDEX] = priceType;
    }

    public void setId(int id) {
        byte[] bytes = byteUtils.intToBytes(id);
        for (int i = 0, j = QUOTE_ID_START_INDEX; i < bytes.length; i++, j++) {
            data[j] = bytes[i];
        }
    }

    public void setChecksum(long checksum) {
        byte[] bytes = byteUtils.longToBytes(checksum);
        for (int i = 0, j = CHECKSUM_INDEX; i < bytes.length; i++, j++) {
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
        setChecksum(checksumUtil.checksum(data));
        paddingToComplete64Bytes();
    }

    private void paddingToComplete64Bytes() {
        for (int i = 57; i < 64; i++) {
            data[i] = 1;
        }
    }


    public byte[] getData() {
        return data;
    }

    public byte[] getDataWithoutChecksum() {
        byte[] dataWithoutChecksum = new byte[OBJ_SIZE];
        System.arraycopy(data, 0, dataWithoutChecksum, 0, OBJ_SIZE);
        for (int i = CHECKSUM_INDEX; i < CHECKSUM_INDEX + CHECKSUM_SIZE_IN_BYTES; i++) {
            dataWithoutChecksum[i] = 0;
        }
        return dataWithoutChecksum;
    }


    public int size() {
        return OBJ_SIZE;
    }
}