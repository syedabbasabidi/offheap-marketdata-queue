package com.abidi.util;

import java.nio.charset.StandardCharsets;

public class Latin1StringUtil {

    public String getStringInLatin1(String src) {
        byte[] gb00BJLR0J16Bytes = src.getBytes(StandardCharsets.ISO_8859_1);
        return new String(gb00BJLR0J16Bytes, StandardCharsets.ISO_8859_1);
    }
}
