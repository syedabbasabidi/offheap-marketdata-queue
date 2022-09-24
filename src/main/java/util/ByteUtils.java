package util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteUtils {

    private static ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
    private static ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
    private static ByteBuffer securityBuffer = ByteBuffer.allocate(12);
    private static ByteBuffer brokerBuffer = ByteBuffer.allocate(3);
    private static ByteBuffer dateBuffer = ByteBuffer.allocate(19);

    public static byte[] longToBytes(long x) {
        longBuffer.putLong(0, x);
        return longBuffer.array();
    }

    public static byte[] intToBytes(int x) {
        intBuffer.putInt(0, x);
        return intBuffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        longBuffer.position(0);
        longBuffer.put(bytes, 0, bytes.length);
        longBuffer.flip();//need flip
        return longBuffer.getLong();
    }

    public static byte bytesToInt(byte[] bytes) {
        intBuffer.position(0);
        intBuffer.put(bytes, 0, bytes.length);
        intBuffer.flip();//need flip
        return intBuffer.get();
    }

    public static String bytesToSecurity(byte[] bytes) {
        securityBuffer.position(0);
        securityBuffer.put(bytes, 0, bytes.length);
        securityBuffer.flip();//need flip
        return new String(securityBuffer.array(), StandardCharsets.UTF_8);
    }
    public static String bytesToBroker(byte[] bytes) {
        brokerBuffer.position(0);
        brokerBuffer.put(bytes, 0, bytes.length);
        brokerBuffer.flip();//need flip
        return new String(brokerBuffer.array(), StandardCharsets.UTF_8);
    }
    public static String bytesToDate(byte[] bytes) {
        dateBuffer.position(0);
        dateBuffer.put(bytes, 0, bytes.length);
        dateBuffer.flip();//need flip
        return new String(dateBuffer.array(), StandardCharsets.UTF_8);
    }

}
