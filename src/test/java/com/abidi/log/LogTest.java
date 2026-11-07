package com.abidi.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LogTest {

    private static final int CAPACITY = 1 << 20;

    @TempDir
    Path dir;

    @Test
    void tailerReadsWhatAppenderWroteAndEndsWhereAppenderEnds() {
        final Path file = dir.resolve("a.log");
        try (LogAppender appender = new LogAppender(file, CAPACITY); LogTailer tailer = new LogTailer(file, CAPACITY, 0)) {
            appender.append(bytes("one"), 0, 3);
            appender.append(bytes("three"), 0, 5);

            assertEquals(List.of("one", "three"), readAll(tailer));
            assertEquals(appender.position(), tailer.position());
        }
    }

    @Test
    void tailerResumesFromPositionAfterLastRecordItHandled() {
        final Path file = dir.resolve("resume.log");
        final long[] resumeAt = new long[1];
        try (LogAppender appender = new LogAppender(file, CAPACITY); LogTailer tailer = new LogTailer(file, CAPACITY, 0)) {
            appender.append(bytes("first"), 0, 5);
            appender.append(bytes("second"), 0, 6);
            tailer.poll((buffer, offset, length, position) -> resumeAt[0] = Records.positionAfter(position, length), 1);
        }
        try (LogTailer tailer = new LogTailer(file, CAPACITY, resumeAt[0])) {
            assertEquals(List.of("second"), readAll(tailer));
        }
    }

    @Test
    void reopenedAppenderContinuesAtEndAndDiscardsTornRecord() {
        final Path file = dir.resolve("b.log");
        try (LogAppender appender = new LogAppender(file, CAPACITY)) {
            appender.append(bytes("first"), 0, 5);
            final int at = appender.claim();
            ByteBuffers.copy(bytes("torn-record-that-never-committed"), 0, appender.buffer(), at, 32);
        }
        try (LogAppender appender = new LogAppender(file, CAPACITY); LogTailer tailer = new LogTailer(file, CAPACITY, 0)) {
            appender.append(bytes("ab"), 0, 2);
            appender.append(bytes("cd"), 0, 2);
            assertEquals(List.of("first", "ab", "cd"), readAll(tailer));
        }
    }

    @Test
    void secondWriterIsRefused() {
        final Path file = dir.resolve("c.log");
        try (LogAppender ignored = new LogAppender(file, CAPACITY)) {
            assertThrows(RuntimeException.class, () -> new LogAppender(file, CAPACITY));
        }
    }

    @Test
    void lengthIsStoredLittleEndianWhateverTheCallerBufferOrder() {
        final Path file = dir.resolve("d.log");
        try (LogAppender appender = new LogAppender(file, CAPACITY)) {
            appender.append(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}).order(ByteOrder.BIG_ENDIAN), 0, 9);
            final byte[] record = new byte[13];
            for (int i = 0; i < record.length; i++) {
                record[i] = appender.buffer().get(i);
            }
            assertArrayEquals(new byte[]{9, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, record);
        }
    }

    @Test
    void counterSurvivesReopen() {
        final Path file = dir.resolve("e.cursor");
        try (MappedCounter counter = new MappedCounter(file)) {
            assertEquals(0, counter.get());
            counter.set(4096);
        }
        try (MappedCounter counter = new MappedCounter(file)) {
            assertEquals(4096, counter.get());
        }
    }

    private static List<String> readAll(final LogTailer tailer) {
        final List<String> out = new ArrayList<>();
        tailer.poll((buffer, offset, length, position) -> out.add(text(buffer, offset, length)), 100);
        return out;
    }

    private static String text(final ByteBuffer buffer, final int offset, final int length) {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = buffer.get(offset + i);
        }
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static ByteBuffer bytes(final String text) {
        return ByteBuffer.wrap(text.getBytes(StandardCharsets.US_ASCII));
    }
}
