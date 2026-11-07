package com.abidi.log;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;

final class Unmapper {

    private static final Unsafe UNSAFE = unsafe();

    private Unmapper() {
    }

    static void unmap(final MappedByteBuffer buffer) {
        if (buffer != null) {
            UNSAFE.invokeCleaner(buffer);
        }
    }

    private static Unsafe unsafe() {
        try {
            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (final ReflectiveOperationException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }
}
