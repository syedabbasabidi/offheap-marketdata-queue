package util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteUtils {

    private static ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
    private static ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
    private static ByteBuffer stringBuffer = ByteBuffer.allocate(17);

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

    public static String bytesToCharSeq(byte[] bytes) {
        stringBuffer.position(0);
        stringBuffer.put(bytes, 0, bytes.length);
        stringBuffer.flip();//need flip
        return new String(stringBuffer.array(), StandardCharsets.UTF_8);
    }

}
