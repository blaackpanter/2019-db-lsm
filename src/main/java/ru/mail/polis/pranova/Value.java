package ru.mail.polis.pranova;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.mail.polis.pranova.Time.currentTime;

public final class Value implements Comparable<Value> {
    private static final AtomicInteger clock = new AtomicInteger();
    private final long timestamp;
    private final ByteBuffer data;

    Value(final long timestamp, final ByteBuffer data) {
        assert timestamp >= 0;
        this.timestamp = timestamp;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(currentTime(), data.duplicate());
    }

    public static Value tombstone() {
        return new Value(currentTime(), null);
    }

    /**
     * Method for get data otherwise get exception.
     *
     * @return data from ByteBuffer
     */
    @NotNull
    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("Removed");
        }
        return data.asReadOnlyBuffer();
    }

    public boolean isRemoved() {
        return data == null;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public long getTimeStamp() {
        return timestamp;
    }
}
