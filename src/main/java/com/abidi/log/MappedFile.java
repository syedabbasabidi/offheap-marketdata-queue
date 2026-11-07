package com.abidi.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

final class MappedFile implements AutoCloseable {

    static final VarHandle INT_IN_BUFFER = MethodHandles.byteBufferViewVarHandle(int[].class, LITTLE_ENDIAN);
    static final VarHandle LONG_IN_BUFFER = MethodHandles.byteBufferViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final Path path;
    private final RandomAccessFile file;
    private final MappedByteBuffer mapped;

    MappedFile(final Path path, final int capacity) {
        this.path = path;
        try {
            Files.createDirectories(path.toAbsolutePath().getParent());
            file = new RandomAccessFile(path.toFile(), "rw");
            if (file.length() < capacity) {
                file.setLength(capacity);
            }
            mapped = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, file.length());
            mapped.order(LITTLE_ENDIAN);
        } catch (final IOException ex) {
            throw new UncheckedIOException("cannot map " + path, ex);
        }
    }

    ByteBuffer buffer() {
        return mapped;
    }

    int capacity() {
        return mapped.capacity();
    }

    Path path() {
        return path;
    }

    FileLock lockExclusively() {
        try {
            final FileLock lock = file.getChannel().tryLock();
            if (lock == null) {
                throw new IllegalStateException("another process is writing " + path);
            }
            return lock;
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void close() {
        Unmapper.unmap(mapped);
        try {
            file.close();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
