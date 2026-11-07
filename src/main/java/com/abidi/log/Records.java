package com.abidi.log;

public final class Records {

    public static final int LENGTH_FIELD = Integer.BYTES;
    public static final int ALIGNMENT = 8;
    public static final int MAX_PAYLOAD = 4096;

    private Records() {
    }

    public static long positionAfter(final long position, final int payloadLength) {
        return position + ((LENGTH_FIELD + payloadLength + ALIGNMENT - 1) & -ALIGNMENT);
    }
}
