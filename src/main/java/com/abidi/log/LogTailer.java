package com.abidi.log;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import static com.abidi.log.MappedFile.INT_IN_BUFFER;
import static com.abidi.log.Records.LENGTH_FIELD;

public final class LogTailer implements AutoCloseable {

    private final MappedFile log;
    private final ByteBuffer buffer;
    private long position;

    public LogTailer(final Path path, final int capacity, final long position) {
        log = new MappedFile(path, capacity);
        buffer = log.buffer();
        this.position = position;
    }

    public long position() {
        return position;
    }

    public int poll(final RecordHandler handler, final int limit) {
        int recordsRead = 0;
        while (recordsRead < limit && position + LENGTH_FIELD <= buffer.capacity()) {
            final int recordPosition = (int) position;
            final int length = (int) INT_IN_BUFFER.getAcquire(buffer, recordPosition);
            final boolean nothingWrittenYet = length == 0;
            if (nothingWrittenYet) {
                break;
            }
            position = Records.positionAfter(recordPosition, length);
            handler.onRecord(buffer, recordPosition + LENGTH_FIELD, length, recordPosition);
            recordsRead++;
        }
        return recordsRead;
    }

    @Override
    public void close() {
        log.close();
    }
}
