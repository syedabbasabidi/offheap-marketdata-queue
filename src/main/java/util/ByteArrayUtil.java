package util;

public class ByteArrayUtil {

    public static void copy(byte[] src, byte[] target) {
        System.arraycopy(src, 0, target, 0, src.length);
    }

    public static void byteToChar(byte[] src, char[] tar) {
        for (int i = 0; i < src.length; i++) {
            tar[i] = (char) src[i];
        }
    }

}
