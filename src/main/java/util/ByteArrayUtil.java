package util;

public class ByteArrayUtil {

    public static void copy(byte[] src, byte[] target) {
        System.arraycopy(src, 0, target, 0, src.length);
    }

}
