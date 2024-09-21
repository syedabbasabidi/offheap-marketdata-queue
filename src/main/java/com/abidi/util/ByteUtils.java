package com.abidi.util;

import java.nio.ByteBuffer;

public class ByteUtils {

    private final ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
    private final ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
    private final ByteBuffer securityBuffer = ByteBuffer.allocate(12);
    private final ByteBuffer brokerBuffer = ByteBuffer.allocate(3);
    private final ByteBuffer dateBuffer = ByteBuffer.allocate(19);

    public byte[] longToBytes(long x) {
        longBuffer.putLong(0, x);
        return longBuffer.array();
    }

    public byte[] intToBytes(int x) {
        intBuffer.putInt(0, x);
        return intBuffer.array();
    }

    public long bytesToLong(byte[] srcBytes, int from, int length) {
        longBuffer.position(0);
        longBuffer.put(srcBytes, from, length);
        longBuffer.flip();//need flip
        return longBuffer.getLong();
    }

    public int bytesToInt(byte[] bytes, int from, int length) {
        intBuffer.position(0);
        intBuffer.put(bytes, from, length);
        intBuffer.flip();//need flip
        return intBuffer.getInt();
    }

    public byte[] bytesToSecurity(byte[] bytes, int from, int length) {
        securityBuffer.position(0);
        securityBuffer.put(bytes, from, length);
        securityBuffer.flip();//need flip
        return securityBuffer.array();
    }

    public byte[] bytesToBroker(byte[] bytes, int from, int length) {
        brokerBuffer.position(0);
        brokerBuffer.put(bytes, from, length);
        brokerBuffer.flip();//need flip
        return brokerBuffer.array();
    }

    public byte[] bytesToDate(byte[] bytes, int from, int length) {
        dateBuffer.position(0);
        dateBuffer.put(bytes, from, length);
        dateBuffer.flip();//need flip
        return dateBuffer.array();
    }

}
