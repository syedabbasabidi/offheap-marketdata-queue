package com.abidi.queue;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface MessageHandler {

    /**
     * @param buffer mapped buffer holding the message, valid only for the duration of the call
     * @param offset absolute index of the message within {@code buffer}
     * @param length message length in bytes (the slot size for {@link CircularMMFQueueFixedSize})
     */
    void onMessage(ByteBuffer buffer, int offset, int length);
}
