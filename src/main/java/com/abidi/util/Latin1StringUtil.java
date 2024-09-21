package com.abidi.util;

import java.nio.charset.StandardCharsets;

public class Latin1StringUtil {

    public String getGb00BJLR0J16InLatin1() {
        String gb00BJLR0J16 = "GB00BJLR0J16";
        byte[] gb00BJLR0J16Bytes = gb00BJLR0J16.getBytes(StandardCharsets.ISO_8859_1);
        gb00BJLR0J16 = new String(gb00BJLR0J16Bytes, StandardCharsets.ISO_8859_1);
        return gb00BJLR0J16;
    }
}
