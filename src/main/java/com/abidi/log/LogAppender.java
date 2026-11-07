package com.abidi.log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.file.Path;

import static com.abidi.log.MappedFile.INT_IN_BUFFER;
import static com.abidi.log.Records.LENGTH_FIELD;
import static com.abidi.log.Records.MAX_PAYLOAD;

public final class LogAppender implements AutoCloseable {

    private static final int NOTHING_CLAIMED = -1;

    private final MappedFile log;
    private final FileLock lock;
    private final ByteBuffer buffer;
    private int position;
    private int claimedPayloadOffset = NOTHING_CLAIMED;

    public LogAppender(final Path path, final int capacity) {
        log = new MappedFile(path, capacity);
        lock = log.lockExclusively();
        buffer = log.buffer();
        position = findEndOfWrittenRecords();
        clearSpaceForNextRecord();
    }

    public long position() {
        return position;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public int claim() {
        requireRoomFor(MAX_PAYLOAD);
        claimedPayloadOffset = position + LENGTH_FIELD;
        return claimedPayloadOffset;
    }

    public long commit(final int length) {
        if (claimedPayloadOffset == NOTHING_CLAIMED || length <= 0 || length > MAX_PAYLOAD) {
            throw new IllegalStateException("invalid commit of " + length + " bytes");
        }
        final int recordPosition = position;
        INT_IN_BUFFER.setRelease(buffer, recordPosition, length);
        position = (int) Records.positionAfter(recordPosition, length);
        claimedPayloadOffset = NOTHING_CLAIMED;
        return recordPosition;
    }

    public long append(final ByteBuffer source, final int offset, final int length) {
        final int payloadOffset = claim();
        ByteBuffers.copy(source, offset, buffer, payloadOffset, length);
        return commit(length);
    }

    private int findEndOfWrittenRecords() {
        int recordPosition = 0;
        while (recordPosition + LENGTH_FIELD <= buffer.capacity()) {
            final int length = (int) INT_IN_BUFFER.getAcquire(buffer, recordPosition);
            final boolean nothingWrittenHere = length == 0;
            if (nothingWrittenHere) {
                break;
            }
            recordPosition = (int) Records.positionAfter(recordPosition, length);
        }
        return recordPosition;
    }

    private void clearSpaceForNextRecord() {
        final int end = Math.min(position + LENGTH_FIELD + MAX_PAYLOAD, buffer.capacity());
        for (int i = position; i < end; i++) {
            buffer.put(i, (byte) 0);
        }
    }

    private void requireRoomFor(final int length) {
        if (position + LENGTH_FIELD + length > buffer.capacity()) {
            throw new IllegalStateException("log full: " + log.path());
        }
    }

    @Override
    public void close() {
        try {
            lock.release();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        log.close();
    }
}
