package com.abidi.util;

import java.util.zip.CRC32;

public class ChecksumUtil {

    public long checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();
    }
}
