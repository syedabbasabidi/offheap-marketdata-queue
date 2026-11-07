package com.abidi.log;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface RecordHandler {

    /**
     * @param buffer   mapped buffer holding the record, valid only for the duration of the call
     * @param offset   absolute index of the record payload within {@code buffer}
     * @param length   payload length in bytes
     * @param position position of the record within its log or queue, used to resume after it
     */
    void onRecord(ByteBuffer buffer, int offset, int length, long position);
}
