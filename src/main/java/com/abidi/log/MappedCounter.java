package com.abidi.log;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static com.abidi.log.MappedFile.LONG_IN_BUFFER;

public final class MappedCounter implements AutoCloseable {

    private static final int SIZE = 64;
    private static final int VALUE_AT = 0;

    private final MappedFile file;
    private final ByteBuffer buffer;

    public MappedCounter(final Path path) {
        file = new MappedFile(path, SIZE);
        buffer = file.buffer();
    }

    public long get() {
        return (long) LONG_IN_BUFFER.getAcquire(buffer, VALUE_AT);
    }

    public void set(final long value) {
        LONG_IN_BUFFER.setRelease(buffer, VALUE_AT, value);
    }

    @Override
    public void close() {
        file.close();
    }
}
