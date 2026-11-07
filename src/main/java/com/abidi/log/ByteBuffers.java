package com.abidi.log;

import java.nio.ByteBuffer;

import static com.abidi.log.MappedFile.LONG_IN_BUFFER;

public final class ByteBuffers {

    private ByteBuffers() {
    }

    public static void copy(final ByteBuffer source, final int sourceOffset, final ByteBuffer target, final int targetOffset, final int length) {
        int i = 0;
        for (; i + Long.BYTES <= length; i += Long.BYTES) {
            LONG_IN_BUFFER.set(target, targetOffset + i, (long) LONG_IN_BUFFER.get(source, sourceOffset + i));
        }
        for (; i < length; i++) {
            target.put(targetOffset + i, source.get(sourceOffset + i));
        }
    }
}
