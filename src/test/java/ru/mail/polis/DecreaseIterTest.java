package ru.mail.polis;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DecreaseIterTest extends TestBase {
    @Test
    public void emptyIterator(@TempDir File data) throws IOException {
        try (DAO dao = DAOFactory.create(data)) {
            final ByteBuffer key = randomKey();
            final ByteBuffer value = randomValue();
            dao.upsert(key, value);
            final ByteBuffer leastKey = ByteBuffer.allocate(0);
            final Iterator<Record> iterator = dao.decreasingIterator(leastKey);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void iterator(@TempDir File data) throws IOException {
        try (DAO dao = DAOFactory.create(data)) {
            final ByteBuffer key = randomKey();
            final ByteBuffer value = randomValue();
            dao.upsert(key, value);
            Record record = dao.decreasingIterator(key).next();
            assertEquals(key, record.getKey());
            assertEquals(value, record.getValue());
        }
    }

    @Test
    public void iteratorManyRecords(@TempDir File data) throws IOException {
        final NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        int n = 100;
        ByteBuffer key = ByteBuffer.allocate(0);
        for (int i = 0; i < n; i++) {
            if (i == 50) {
                key = randomKey();
                map.put(key, randomValue());
            } else {
                map.put(randomKey(), randomValue());
            }
        }
        try (DAO dao = DAOFactory.create(data)) {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry :
                    map.entrySet()) {
                dao.upsert(entry.getKey(), entry.getValue());
            }
            checkCorrect(dao, map, key);
        }
    }

    private void checkCorrect(@NotNull final DAO dao,
                              @NotNull final NavigableMap<ByteBuffer, ByteBuffer> map,
                              @NotNull final ByteBuffer from) throws IOException {
        final Iterator<Record> iterator = dao.decreasingIterator(from);
        for (Map.Entry<ByteBuffer, ByteBuffer> entry :
                map.headMap(from, true).descendingMap().entrySet()) {
            final Record record = iterator.next();
            assertEquals(entry.getKey(), record.getKey());
            assertEquals(entry.getValue(), record.getValue());
        }
    }

    @Test
    public void reverseFullIterator(@TempDir File data) throws IOException {
        NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        int n = 100;
        for (int i = 0; i < n; i++) {
            map.put(randomKey(), randomValue());
        }
        try (DAO dao = DAOFactory.create(data)) {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry :
                    map.entrySet()) {
                dao.upsert(entry.getKey(), entry.getValue());
            }
            checkCorrect(dao, map, map.lastKey());
        }
    }

    @Test
    public void iteratorAfterFlush(@TempDir File data) throws IOException {
        NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        int n = 100;
        for (int i = 0; i < n; i++) {
            map.put(randomKey(), randomValue());
        }

        try (DAO dao = DAOFactory.create(data)) {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry :
                    map.entrySet()) {
                dao.upsert(entry.getKey(), entry.getValue());
            }
        }

        try (DAO dao = DAOFactory.create(data)) {
            checkCorrect(dao, map, map.lastKey());
        }
    }

    @Test
    public void iteratorAfterCompaction(@TempDir File data) throws IOException {
        NavigableMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        int n = 100;
        for (int i = 0; i < n; i++) {
            map.put(randomKey(), randomValue());
        }

        try (DAO dao = DAOFactory.create(data)) {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry :
                    map.entrySet()) {
                dao.upsert(entry.getKey(), entry.getValue());
            }
        }

        for (int i = 0; i < n; i++) {
            map.put(randomKey(), randomValue());
        }

        try (DAO dao = DAOFactory.create(data)) {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry :
                    map.entrySet()) {
                dao.upsert(entry.getKey(), entry.getValue());
            }
        }

        try (DAO dao = DAOFactory.create(data)) {
            dao.compact();
        }

        try (DAO dao = DAOFactory.create(data)) {
            checkCorrect(dao, map, map.lastKey());
        }
    }
}
