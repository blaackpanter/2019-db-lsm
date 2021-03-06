package ru.mail.polis.pranova;

import java.nio.ByteBuffer;

public final class Bytes {
    private Bytes() {
    }

    /**
     * Return ByteBuffer from int value.
     *
     * @param value for ByteBuffer
     * @return ByteBuffer
     */
    public static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        result.putInt(value);
        result.rewind();
        return result;
    }

    /**
     * Return ByteBuffer from long value.
     * *
     * * @param value for ByteBuffer
     * * @return ByteBuffer
     */
    public static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        result.putLong(value);
        result.rewind();
        return result;
    }
}
